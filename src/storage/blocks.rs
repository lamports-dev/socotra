use {
    crate::{
        metrics::{BUILD_READER_STATE_SECONDS, STORED_SLOT, WRITE_BLOCK_SYNC_SECONDS},
        source::grpc::GeyserMessage,
        storage::{
            reader::{Reader, ReaderState},
            rocksdb::{Rocksdb, SlotIndexValue},
        },
    },
    ahash::HashMap,
    anyhow::Context,
    metrics::{gauge, histogram},
    richat_metrics::duration_to_seconds,
    richat_proto::geyser::SlotStatus,
    serde::{Deserialize, Serialize},
    solana_sdk::{
        account::Account,
        clock::{MAX_PROCESSING_AGE, Slot},
        hash::Hash,
        pubkey::Pubkey,
    },
    std::{
        collections::{BTreeMap, VecDeque},
        path::PathBuf,
        sync::Arc,
        thread,
        time::{Duration, Instant},
    },
    tokio::{
        fs,
        io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
        sync::{mpsc, oneshot},
    },
    tokio_util::sync::CancellationToken,
    tracing::{info, info_span, instrument, warn},
};

#[derive(Debug)]
struct Block {
    height: Option<Slot>,
    blockhash: Option<Hash>,
    accounts: HashMap<Pubkey, Arc<Account>>,
    confirmed: bool,
    dead: bool,
}

impl Default for Block {
    fn default() -> Self {
        Self {
            height: None,
            blockhash: None,
            accounts: HashMap::with_capacity_and_hasher(8_192, Default::default()),
            confirmed: false,
            dead: false,
        }
    }
}

#[derive(Serialize, Deserialize)]
enum DiskMessage {
    Reset,
    Slot {
        slot: Slot,
        status: i32,
    },
    Block {
        slot: Slot,
        height: Slot,
        blockhash: [u8; 32],
        accounts: Vec<(Pubkey, Account)>,
    },
    AccountAfterBlock {
        slot: Slot,
        pubkey: Pubkey,
        account: Account,
    },
}

impl From<&GeyserMessage> for DiskMessage {
    fn from(msg: &GeyserMessage) -> Self {
        match msg {
            GeyserMessage::Reset => DiskMessage::Reset,
            GeyserMessage::Slot { slot, status } => DiskMessage::Slot {
                slot: *slot,
                status: *status as i32,
            },
            GeyserMessage::Block {
                slot,
                height,
                blockhash,
                accounts,
            } => DiskMessage::Block {
                slot: *slot,
                height: *height,
                blockhash: blockhash.to_bytes(),
                accounts: accounts
                    .iter()
                    .map(|(k, v)| (*k, Account::clone(v)))
                    .collect(),
            },
            GeyserMessage::AccountAfterBlock {
                slot,
                pubkey,
                account,
            } => DiskMessage::AccountAfterBlock {
                slot: *slot,
                pubkey: *pubkey,
                account: Account::clone(account),
            },
        }
    }
}

impl TryFrom<DiskMessage> for GeyserMessage {
    type Error = anyhow::Error;

    fn try_from(msg: DiskMessage) -> anyhow::Result<Self> {
        Ok(match msg {
            DiskMessage::Reset => GeyserMessage::Reset,
            DiskMessage::Slot { slot, status } => GeyserMessage::Slot {
                slot,
                status: SlotStatus::try_from(status)
                    .map_err(|_| anyhow::anyhow!("invalid SlotStatus: {status}"))?,
            },
            DiskMessage::Block {
                slot,
                height,
                blockhash,
                accounts,
            } => GeyserMessage::Block {
                slot,
                height,
                blockhash: Hash::new_from_array(blockhash),
                accounts: accounts
                    .into_iter()
                    .map(|(k, v)| (k, Arc::new(v)))
                    .collect(),
            },
            DiskMessage::AccountAfterBlock {
                slot,
                pubkey,
                account,
            } => GeyserMessage::AccountAfterBlock {
                slot,
                pubkey,
                account: Arc::new(account),
            },
        })
    }
}

struct DiskBuffer {
    writer: BufWriter<fs::File>,
    path: PathBuf,
    count: u64,
}

impl DiskBuffer {
    async fn new(path: PathBuf) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create dir {}", parent.display()))?;
        }
        let file = fs::File::create(&path)
            .await
            .with_context(|| format!("failed to create {}", path.display()))?;
        Ok(Self {
            writer: BufWriter::with_capacity(8 * 1024 * 1024, file),
            path,
            count: 0,
        })
    }

    async fn push(&mut self, msg: &GeyserMessage) -> anyhow::Result<()> {
        let disk_msg = DiskMessage::from(msg);
        let bytes = bincode::serialize(&disk_msg).context("failed to serialize message")?;
        let len = bytes.len() as u32;
        self.writer
            .write_all(&len.to_le_bytes())
            .await
            .context("failed to write length")?;
        self.writer
            .write_all(&bytes)
            .await
            .context("failed to write message")?;
        self.count += 1;
        Ok(())
    }

    async fn into_reader(mut self) -> anyhow::Result<DiskBufferReader> {
        self.writer
            .flush()
            .await
            .context("failed to flush disk buffer")?;
        let file = fs::File::open(&self.path)
            .await
            .with_context(|| format!("failed to open {}", self.path.display()))?;
        // self drops here → Drop removes file from filesystem.
        // Reader's open fd keeps data accessible (Linux).
        Ok(DiskBufferReader {
            reader: BufReader::with_capacity(8 * 1024 * 1024, file),
            remaining: self.count,
        })
    }
}

impl Drop for DiskBuffer {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path) {
            warn!(
                path = %self.path.display(),
                %error,
                "failed to remove disk buffer file"
            );
        }
    }
}

struct DiskBufferReader {
    reader: BufReader<fs::File>,
    remaining: u64,
}

impl DiskBufferReader {
    async fn next_message(&mut self) -> Option<anyhow::Result<GeyserMessage>> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;

        let mut len_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut len_buf).await {
            return Some(Err(e).context("failed to read message length"));
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut msg_buf = vec![0u8; len];
        if let Err(e) = self.reader.read_exact(&mut msg_buf).await {
            return Some(Err(e).context("failed to read message bytes"));
        }

        let disk_msg: DiskMessage = match bincode::deserialize(&msg_buf) {
            Ok(m) => m,
            Err(e) => return Some(Err(e).context("failed to deserialize message")),
        };
        Some(GeyserMessage::try_from(disk_msg))
    }
}

pub async fn start(
    init_buffer_path: PathBuf,
    mut db_ready_fut: impl Future<Output = anyhow::Result<Rocksdb>> + Unpin,
    mut latest_stored_slot: SlotIndexValue,
    mut geyser_update_rx: mpsc::Receiver<GeyserMessage>,
    reader: Reader,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut slots = BTreeMap::<Slot, Block>::default();
    let mut disk_buffer = DiskBuffer::new(init_buffer_path).await?;
    let mut messages = VecDeque::new();

    // Buffer messages while DB initializes
    let db = loop {
        tokio::select! {
            db_result = &mut db_ready_fut => break db_result?,
            msg = geyser_update_rx.recv() => match msg {
                Some(msg) => {
                    disk_buffer.push(&msg).await?;
                }
                None => {
                    anyhow::ensure!(shutdown.is_cancelled(), "failed to get message from update channel");
                    return Ok(());
                }
            },
            () = shutdown.cancelled() => return Ok(()),
        }
    };

    // Load finalized blockhashes from DB
    let mut finalized_blockhashes = db.load_blockhashes()?;
    info!(
        count = finalized_blockhashes.len(),
        "loaded blockhashes from db"
    );

    // Replay buffered messages
    let mut disk_reader = if disk_buffer.count > 0 {
        info!(
            msg_count = disk_buffer.count,
            "replaying messages from disk buffer"
        );
        Some(disk_buffer.into_reader().await?)
    } else {
        drop(disk_buffer);
        None
    };

    // Dedicated thread for blocking rocksdb writes
    let (store_tx, store_rx) = std::sync::mpsc::channel::<StoreRequest>();
    let store_thread = thread::Builder::new()
        .name("socBlocksStore".to_owned())
        .spawn({
            let db = db.clone();
            move || {
                for req in store_rx {
                    let result =
                        db.store_new_state(req.slot_info, req.accounts.into_iter(), req.blockhash);
                    let _ = req.result_tx.send(result);
                }
            }
        })
        .context("failed to spawn store thread")?;

    // Process messages: replay from disk first, then switch to live channel
    let replay_ts = Instant::now();
    let replay_start_slot = latest_stored_slot.slot;
    while !shutdown.is_cancelled() {
        if disk_reader.is_some() || !messages.is_empty() {
            let ts = Instant::now();
            while ts.elapsed() < Duration::from_millis(400) {
                let msg = if let Some(reader) = &mut disk_reader {
                    match reader.next_message().await {
                        Some(result) => Some(result?),
                        None => {
                            let blocks_added = latest_stored_slot.slot - replay_start_slot;
                            info!(blocks_added, elapsed = ?replay_ts.elapsed(), "disk buffer replay complete");
                            disk_reader = None;
                            messages.pop_front()
                        }
                    }
                } else {
                    messages.pop_front()
                };
                match msg {
                    Some(msg) => {
                        process_message(
                            &store_tx,
                            &mut latest_stored_slot,
                            &mut slots,
                            &mut finalized_blockhashes,
                            msg,
                        )
                        .await?
                    }
                    None => break,
                }
            }
            let ts = Instant::now();
            let state = build_reader_state(&slots, &latest_stored_slot, &finalized_blockhashes)?;
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

    drop(store_tx);
    store_thread
        .join()
        .map_err(|_| anyhow::anyhow!("store thread panicked"))?;

    Ok(())
}

struct StoreRequest {
    slot_info: SlotIndexValue,
    accounts: HashMap<Pubkey, Arc<Account>>,
    blockhash: Hash,
    result_tx: oneshot::Sender<anyhow::Result<()>>,
}

async fn process_message(
    store_tx: &std::sync::mpsc::Sender<StoreRequest>,
    latest_stored_slot: &mut SlotIndexValue,
    slots: &mut BTreeMap<Slot, Block>,
    finalized_blockhashes: &mut BTreeMap<Slot, Hash>,
    msg: GeyserMessage,
) -> anyhow::Result<()> {
    match msg {
        GeyserMessage::Reset => {
            // Keep finalized entries (backed by DB), clear in-memory above finalized height
            finalized_blockhashes.split_off(&(latest_stored_slot.height + 1));
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

                    let blockhash = block
                        .blockhash
                        .ok_or_else(|| anyhow::anyhow!("no blockhash for finalized slot#{slot}"))?;

                    *latest_stored_slot = SlotIndexValue { slot, height };

                    // Update finalized blockhashes and prune old
                    finalized_blockhashes.insert(height, blockhash);
                    let min_height = height.saturating_sub(MAX_PROCESSING_AGE as u64 - 1);
                    // Remove entries below min_height
                    *finalized_blockhashes = finalized_blockhashes.split_off(&min_height);

                    // store new slot on dedicated thread
                    let (result_tx, result_rx) = oneshot::channel();
                    let ts = Instant::now();
                    store_tx
                        .send(StoreRequest {
                            slot_info: *latest_stored_slot,
                            accounts: block.accounts,
                            blockhash,
                            result_tx,
                        })
                        .map_err(|_| anyhow::anyhow!("store thread gone"))?;
                    result_rx.await.context("store thread panicked")??;
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
            blockhash,
            accounts,
        } => {
            anyhow::ensure!(
                slot > latest_stored_slot.slot,
                "received Block message after Finalized"
            );

            let block = slots.entry(slot).or_default();
            block.height = Some(height);
            block.blockhash = Some(blockhash);
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
    finalized_blockhashes: &BTreeMap<Slot, Hash>,
) -> anyhow::Result<ReaderState> {
    // Confirmed: heights must be strictly incremental
    let mut confirmed_slot = latest_stored_slot.slot;
    let mut confirmed_map = HashMap::with_capacity_and_hasher(65_536, Default::default());
    let mut expected_confirmed_height = latest_stored_slot.height + 1;
    let mut confirmed_blockhashes = Vec::with_capacity(32);

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
                if let Some(bh) = block.blockhash {
                    confirmed_blockhashes.push((height, bh));
                }
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
    let mut processed_blockhashes = Vec::with_capacity(2);

    let processed_height = {
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
            if let Some(bh) = block.blockhash {
                processed_blockhashes.push((height, bh));
            }
            next_height = height + 1;
        }

        next_height - 1
    };

    // Build blockhash -> height map from finalized + confirmed + processed,
    // filtered to the valid age window
    let min_height = processed_height.saturating_sub(MAX_PROCESSING_AGE as u64 - 1);
    let blockhash_map = finalized_blockhashes
        .range(min_height..=processed_height)
        .map(|(height, hash)| (*height, *hash))
        .chain(confirmed_blockhashes.into_iter())
        .chain(processed_blockhashes.into_iter())
        .map(|(height, hash)| (hash, height))
        .collect();

    gauge!(STORED_SLOT, "commitment" => "processed").set(processed_slot as f64);
    gauge!(STORED_SLOT, "commitment" => "confirmed").set(confirmed_slot as f64);
    gauge!(STORED_SLOT, "commitment" => "finalized").set(latest_stored_slot.slot as f64);

    Ok(ReaderState {
        processed_slot,
        processed_height,
        processed_map,
        confirmed_slot,
        confirmed_map,
        finalized_slot: latest_stored_slot.slot,
        blockhash_map,
    })
}
