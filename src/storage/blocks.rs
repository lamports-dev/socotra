use {
    crate::{
        metrics::{BUILD_READER_STATE_SECONDS, WRITE_BLOCK_SYNC_SECONDS},
        source::grpc::GeyserMessage,
        storage::{
            reader::{Reader, ReaderState},
            rocksdb::{Rocksdb, SlotIndexValue},
        },
    },
    ahash::HashMap,
    anyhow::Context,
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
    tracing::{info_span, instrument},
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
    mut geyser_update_rx: mpsc::Receiver<GeyserMessage>,
    reader: Reader,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut messages = VecDeque::new();
    let db = loop {
        tokio::select! {
            db_result = &mut db_ready_fut => break db_result?,
            msg = geyser_update_rx.recv() => match msg {
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
            let ts = Instant::now();
            let state = build_reader_state(&slots, &latest_stored_slot)?;
            histogram!(BUILD_READER_STATE_SECONDS).record(duration_to_seconds(ts.elapsed()));
            reader
                .update(Arc::new(state))
                .context("failed to update reader")?;
        }

        tokio::select! {
            msg = geyser_update_rx.recv() => match msg {
                Some(msg) => {
                    messages.push_back(msg);
                    while let Ok(msg) = geyser_update_rx.try_recv() {
                        messages.push_back(msg);
                    }
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
                SlotStatus::SlotDead => {
                    block.dead = true;
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

#[instrument(skip_all, fields(slot = latest_stored_slot.slot))]
fn build_reader_state(
    slots: &BTreeMap<Slot, Block>,
    latest_stored_slot: &SlotIndexValue,
) -> anyhow::Result<ReaderState> {
    // Confirmed: heights must be strictly incremental
    let mut confirmed_slot = latest_stored_slot.slot;
    let mut confirmed_map = HashMap::with_capacity_and_hasher(65_536, Default::default());
    let mut expected_confirmed_height = latest_stored_slot.height + 1;

    {
        let _span = info_span!("confirmed").entered();

        for (&slot, block) in slots.iter() {
            if block.dead || !block.confirmed {
                continue;
            }
            if let Some(height) = block.height {
                anyhow::ensure!(
                    height == expected_confirmed_height,
                    "confirmed height mismatch at slot#{slot}: expected {expected_confirmed_height}, got {height}"
                );
                expected_confirmed_height = height + 1;
            }
            confirmed_slot = slot;
            for (&pubkey, account) in &block.accounts {
                confirmed_map.insert(pubkey, Arc::clone(account));
            }
        }
    }

    // Processed: select the longest branch among forks.
    // Multiple slots can share the same height (fork). We pick the highest
    // slot at each height level, which represents the latest fork tip.
    let mut processed_slot = confirmed_slot;
    let mut processed_map = HashMap::with_capacity_and_hasher(8_192, Default::default());

    {
        let _span = info_span!("processed").entered();

        // Group unconfirmed non-dead blocks by height, keeping the highest slot per height
        let mut by_height = BTreeMap::<Slot, (Slot, &Block)>::new();
        for (&slot, block) in slots.iter() {
            if block.dead || block.confirmed {
                continue;
            }
            if let Some(height) = block.height {
                // Later slot (higher) overwrites earlier at same height
                by_height.insert(height, (slot, block));
            }
        }

        // Walk consecutive heights from confirmed tip
        let mut next_height = expected_confirmed_height;
        for (&height, &(slot, block)) in by_height.iter() {
            if height != next_height {
                break;
            }
            processed_slot = slot;
            for (&pubkey, account) in &block.accounts {
                processed_map.insert(pubkey, Arc::clone(account));
            }
            next_height = height + 1;
        }
    }

    Ok(ReaderState {
        processed_slot,
        processed_map,
        confirmed_slot,
        confirmed_map,
        finalized_slot: latest_stored_slot.slot,
    })
}
