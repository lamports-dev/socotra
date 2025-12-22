use {
    crate::{
        grpc::GeyserMessage,
        rocksdb::{Rocksdb, SlotIndexValue},
    },
    foldhash::quality::RandomState,
    richat_proto::geyser::SlotStatus,
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::{
        collections::{BTreeMap, HashMap, VecDeque, hash_map::Entry as HashMapEntry},
        sync::Arc,
        time::Instant,
    },
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
    tracing::debug,
};

#[derive(Debug)]
struct VersionedAccount {
    version: u64,
    account: Arc<Account>,
}

#[derive(Debug)]
struct Block {
    height: Option<Slot>,
    accounts: HashMap<Pubkey, VersionedAccount, RandomState>,
    confirmed: bool,
    dead: bool,
}

impl Default for Block {
    fn default() -> Self {
        Self {
            height: None,
            accounts: HashMap::with_capacity_and_hasher(8_192, RandomState::default()),
            confirmed: false,
            dead: false,
        }
    }
}

pub async fn start(
    mut db_ready_fut: impl Future<Output = anyhow::Result<Rocksdb>> + Unpin,
    mut latest_stored_slot: SlotIndexValue,
    mut update_rx: mpsc::Receiver<GeyserMessage>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut messages = VecDeque::new();
    let db = loop {
        tokio::select! {
            biased;
            db_result = &mut db_ready_fut => break db_result?,
            msg = update_rx.recv() => match msg {
                Some(msg) => messages.push_back(msg),
                None => {
                    anyhow::ensure!(shutdown.is_cancelled(), "failed to get message from update channel");
                    return Ok(());
                }
            },
            () = shutdown.cancelled() => return Ok(()),
        }
    };

    let mut slots = BTreeMap::<Slot, Block>::default();
    loop {
        for _ in 0..100 {
            match messages.pop_front() {
                Some(msg) => process_message(&db, &mut latest_stored_slot, &mut slots, msg)?,
                None => break,
            }
        }

        tokio::select! {
            biased;
            msg = update_rx.recv() => match msg {
                Some(msg) => if messages.is_empty() {
                    process_message(&db, &mut latest_stored_slot, &mut slots, msg)?;
                } else {
                    messages.push_back(msg)
                },
                None => {
                    anyhow::ensure!(shutdown.is_cancelled(), "failed to get message from update channel");
                    return Ok(());
                }
            },
            () = shutdown.cancelled() => break,
        };
    }

    Ok(())
}

fn process_message(
    db: &Rocksdb,
    latest_stored_slot: &mut SlotIndexValue,
    slots: &mut BTreeMap<Slot, Block>,
    msg: GeyserMessage,
) -> anyhow::Result<()> {
    match msg {
        GeyserMessage::Account {
            pubkey,
            slot,
            write_version: version,
            account,
        } => {
            anyhow::ensure!(
                slot > latest_stored_slot.slot,
                "slot for received account already stored"
            );

            let block = slots.entry(slot).or_default();
            match block.accounts.entry(pubkey) {
                HashMapEntry::Occupied(mut entry) => {
                    let entry = entry.get_mut();
                    if entry.version < version {
                        entry.version = version;
                        entry.account = account;
                    }
                }
                HashMapEntry::Vacant(entry) => {
                    entry.insert(VersionedAccount { version, account });
                }
            }
        }
        GeyserMessage::BlockMeta { slot, height } => {
            let block = slots.entry(slot).or_default();
            block.height = Some(height);
        }
        GeyserMessage::Slot { slot, status } => {
            let block = slots.entry(slot).or_default();
            if status == SlotStatus::SlotDead {
                block.dead = true;
            } else if status == SlotStatus::SlotConfirmed {
                block.confirmed = true
            } else if status == SlotStatus::SlotFinalized && slot > latest_stored_slot.slot {
                slots.retain(|bank_slot, _bank| *bank_slot >= slot);
                let Some(bank) = slots.remove(&slot) else {
                    anyhow::bail!("no finalized slot info for slot#{slot}");
                };
                anyhow::ensure!(!bank.dead, "finalized slot#{slot} marked as dead");

                let Some(height) = bank.height else {
                    anyhow::bail!("no height for finalized slot#{slot}");
                };
                anyhow::ensure!(
                    latest_stored_slot.height + 1 == height,
                    "height mismatch: {} + 1 == {height}",
                    latest_stored_slot.height
                );
                *latest_stored_slot = SlotIndexValue { slot, height };

                let ts = Instant::now();
                db.store_new_state(
                    *latest_stored_slot,
                    bank.accounts
                        .into_iter()
                        .map(|(pubkey, data)| (pubkey, data.account)),
                )?;
                debug!(slot, elapsed = ?ts.elapsed(), "save finalized slot");
            }
        }
        GeyserMessage::Reset => {
            slots.retain(|_bank_slot, bank| bank.confirmed);
        }
    }

    Ok(())
}
