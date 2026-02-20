use {
    crate::{
        metrics::WRITE_BLOCK_SYNC_SECONDS,
        source::grpc::GeyserMessage,
        storage::rocksdb::{Rocksdb, SlotIndexValue},
    },
    ahash::HashMap,
    metrics::histogram,
    richat_metrics::duration_to_seconds,
    richat_proto::geyser::SlotStatus,
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::{
        collections::{BTreeMap, VecDeque},
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
};

#[derive(Debug)]
struct Block {
    height: Option<Slot>,
    accounts: HashMap<Pubkey, Arc<Account>>,
    confirmed: bool,
    dead: bool,
}

impl Default for Block {
    fn default() -> Self {
        Self {
            height: None,
            accounts: HashMap::with_capacity_and_hasher(8_192, Default::default()),
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
        if !messages.is_empty() {
            let ts = Instant::now();
            while ts.elapsed() < Duration::from_millis(400) {
                match messages.pop_front() {
                    Some(msg) => process_message(&db, &mut latest_stored_slot, &mut slots, msg)?,
                    None => break,
                }
            }
        }

        tokio::select! {
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
        GeyserMessage::Reset => {
            slots.clear();
        }
        GeyserMessage::Slot { slot, status } => {
            anyhow::ensure!(
                slot > latest_stored_slot.slot,
                "received Slot message after Finalized"
            );

            let block = slots.entry(slot).or_default();
            match status {
                SlotStatus::SlotDead => {
                    block.dead = true;
                }
                SlotStatus::SlotConfirmed => {
                    block.confirmed = true;
                }
                SlotStatus::SlotFinalized => {
                    // remove old slots
                    loop {
                        match slots.keys().next().copied() {
                            Some(block_slot) if block_slot < slot => slots.remove(&block_slot),
                            _ => break,
                        };
                    }

                    // get block
                    let Some(block) = slots.remove(&slot) else {
                        anyhow::bail!("no finalized slot info for slot#{slot}");
                    };
                    anyhow::ensure!(!block.dead, "finalized slot#{slot} marked as dead");

                    // update latest info
                    let Some(height) = block.height else {
                        anyhow::bail!("no height for finalized slot#{slot}");
                    };
                    anyhow::ensure!(
                        latest_stored_slot.height + 1 == height,
                        "height mismatch: {} + 1 == {height}",
                        latest_stored_slot.height
                    );
                    *latest_stored_slot = SlotIndexValue { slot, height };

                    // store new slot
                    let ts = Instant::now();
                    db.store_new_state(*latest_stored_slot, block.accounts.into_iter())?;
                    histogram!(WRITE_BLOCK_SYNC_SECONDS).record(duration_to_seconds(ts.elapsed()));
                }
                _ => {}
            }
        }
        GeyserMessage::Block {
            slot,
            height,
            accounts,
        } => {
            anyhow::ensure!(
                slot > latest_stored_slot.slot,
                "received Block message after Finalized"
            );

            let block = slots.entry(slot).or_default();
            block.height = Some(height);
            block.accounts = accounts;
        }
        GeyserMessage::AccountAfterBlock {
            slot,
            pubkey,
            account,
        } => {
            anyhow::ensure!(
                slot > latest_stored_slot.slot,
                "received AccountAfterBlock message after Finalized"
            );

            let block = slots.entry(slot).or_default();
            block.accounts.insert(pubkey, account);
        }
    }

    Ok(())
}
