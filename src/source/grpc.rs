use {
    crate::config::ConfigSource,
    ahash::HashMap,
    anyhow::Context,
    futures::stream::StreamExt,
    maplit::hashmap,
    richat_client::{error::ReceiveError, grpc::ConfigGrpcClient, stream::SubscribeStream},
    richat_proto::geyser::{
        CommitmentLevel as CommitmentLevelProto, SlotStatus, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterSlots, subscribe_update::UpdateOneof,
    },
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::{
        collections::{BTreeMap, hash_map::Entry as HashMapEntry},
        sync::Arc,
    },
    tokio::{sync::mpsc, time::sleep},
    tokio_util::sync::CancellationToken,
    tonic::Code,
    tracing::{error, info},
};

#[derive(Debug)]
pub enum GeyserMessage {
    // Created on every connect
    Reset,
    // Confirmed, Finalized, Dead
    Slot {
        slot: Slot,
        status: SlotStatus,
    },
    // Created on BlockMeta (means processed)
    Block {
        slot: Slot,
        height: Slot,
        accounts: HashMap<Pubkey, Arc<Account>>,
    },
    // Created when account update received after BlockMeta
    AccountAfterBlock {
        slot: Slot,
        pubkey: Pubkey,
        account: Arc<Account>,
    },
}

#[derive(Debug, Default)]
struct SlotInfo {
    height: Option<Slot>,
    accounts: HashMap<Pubkey, (u64, Arc<Account>)>,
}

pub async fn subscribe(
    update_tx: mpsc::Sender<GeyserMessage>,
    config: ConfigSource,
    mut replay_from_slot: Slot,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut backoff_duration = config.reconnect.map(|c| c.backoff_init);
    let backoff_max = config.reconnect.map(|c| c.backoff_max).unwrap_or_default();

    while !shutdown.is_cancelled() {
        let mut stream = loop {
            tokio::select! {
                _ = shutdown.cancelled() => continue,
                result = subscribe_once(config.config.clone(), replay_from_slot) => match result {
                    Ok(stream) => break stream,
                    Err(error) => {
                        if let Some(sleep_duration) = backoff_duration {
                            error!(?error, "failed to connect to gRPC stream");
                            tokio::select! {
                                _ = shutdown.cancelled() => continue,
                                _ = sleep(sleep_duration) => {}
                            }
                            backoff_duration = Some((sleep_duration * 2).min(backoff_max));
                        } else {
                            return Err(error);
                        }
                    }
                },
            };
        };
        backoff_duration = config.reconnect.map(|c| c.backoff_init);

        anyhow::ensure!(
            update_tx.send(GeyserMessage::Reset).await.is_ok() || shutdown.is_cancelled(),
            "update channel is closed"
        );

        let mut slots = BTreeMap::<Slot, SlotInfo>::new();
        loop {
            let update = tokio::select! {
                _ = shutdown.cancelled() => break,
                result = stream.next() => match result {
                    Some(Ok(update)) => update,
                    Some(Err(error)) => {
                        if matches!(&error, ReceiveError::Status(status)
                            if (status.code() == Code::InvalidArgument && status.message().contains("replay")) || status.code() == Code::DataLoss)
                        {
                            anyhow::bail!("failed to replay from_slot: {replay_from_slot}")
                        } else {
                            error!(%error, "failed to get message from gRPC stream");
                            break;
                        }
                    }
                    None => {
                        error!("gRPC stream is finished");
                        break;
                    },
                }
            };

            let msg = match update.update_oneof {
                Some(UpdateOneof::Account(update)) => {
                    if update.slot < replay_from_slot {
                        continue;
                    }

                    let Some(account) = update.account else {
                        error!("missed account update");
                        break;
                    };

                    let Ok(pubkey) = Pubkey::try_from(account.pubkey) else {
                        error!("invalid pubkey on account update");
                        break;
                    };

                    let Ok(owner) = Pubkey::try_from(account.owner) else {
                        error!("invalid owner on account update");
                        break;
                    };

                    let write_version = account.write_version;
                    let account = Arc::new(Account {
                        lamports: account.lamports,
                        data: account.data,
                        owner,
                        executable: account.executable,
                        rent_epoch: account.rent_epoch,
                    });
                    let mut msg_account = None;

                    let slot_info = slots.entry(update.slot).or_default();
                    match slot_info.accounts.entry(pubkey) {
                        HashMapEntry::Occupied(mut entry) => {
                            let entry = entry.get_mut();
                            if write_version > entry.0 {
                                msg_account = Some(Arc::clone(&account));
                                *entry = (write_version, account);
                            }
                        }
                        HashMapEntry::Vacant(entry) => {
                            msg_account = Some(Arc::clone(&account));
                            entry.insert((write_version, account));
                        }
                    }

                    if let Some(account) = msg_account {
                        GeyserMessage::AccountAfterBlock {
                            slot: update.slot,
                            pubkey,
                            account,
                        }
                    } else {
                        continue;
                    }
                }
                Some(UpdateOneof::BlockMeta(update)) => {
                    if update.slot < replay_from_slot {
                        continue;
                    }

                    let Some(height) = update.block_height.map(|value| value.block_height) else {
                        continue;
                    };

                    let slot_info = slots.entry(update.slot).or_default();
                    if slot_info.height.is_some() {
                        continue;
                    }
                    slot_info.height = Some(height);

                    GeyserMessage::Block {
                        slot: update.slot,
                        height,
                        accounts: slot_info
                            .accounts
                            .iter()
                            .map(|(pk, (_write_version, account))| (*pk, Arc::clone(account)))
                            .collect(),
                    }
                }
                Some(UpdateOneof::Slot(update)) => {
                    if update.slot < replay_from_slot
                        || !matches!(
                            update.status(),
                            SlotStatus::SlotConfirmed
                                | SlotStatus::SlotFinalized
                                | SlotStatus::SlotDead
                        )
                    {
                        continue;
                    }

                    if update.status() == SlotStatus::SlotFinalized {
                        replay_from_slot = replay_from_slot.max(update.slot + 1);

                        loop {
                            match slots.keys().next().copied() {
                                Some(slot) if slot < update.slot => slots.remove(&slot),
                                _ => break,
                            };
                        }
                    }

                    GeyserMessage::Slot {
                        slot: update.slot,
                        status: update.status(),
                    }
                }
                _ => continue,
            };

            anyhow::ensure!(
                update_tx.send(msg).await.is_ok() || shutdown.is_cancelled(),
                "update channel is closed"
            );
        }
    }

    Ok(())
}

async fn subscribe_once(
    config: ConfigGrpcClient,
    replay_from_slot: Slot,
) -> anyhow::Result<SubscribeStream> {
    let mut connection = config
        .connect()
        .await
        .context("failed to connect to gRPC service")?;

    let version = connection
        .get_version()
        .await
        .context("failed to get gRPC service version")?;
    info!(version = version.version, "connected to gRPC stream");

    connection
        .subscribe_dragons_mouth_once(SubscribeRequest {
            accounts: hashmap! { "".to_owned() => SubscribeRequestFilterAccounts::default() },
            slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            } },
            transactions: hashmap! {},
            transactions_status: hashmap! {},
            blocks: hashmap! {},
            blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta::default() },
            entry: hashmap! {},
            commitment: Some(CommitmentLevelProto::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: Some(replay_from_slot),
        })
        .await
        .context("failed to subscribe")
        .map(|stream| stream.into_parsed())
}
