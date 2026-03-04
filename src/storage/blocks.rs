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
    serde::{Deserialize, Serialize},
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::{
        collections::{BTreeMap, VecDeque},
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        fs,
        io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
        sync::mpsc,
    },
    tokio_util::sync::CancellationToken,
    tracing::{info, info_span, instrument, warn},
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
                accounts,
            } => DiskMessage::Block {
                slot: *slot,
                height: *height,
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
                accounts,
            } => GeyserMessage::Block {
                slot,
                height,
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
    init_buffer_path: Option<PathBuf>,
    mut db_ready_fut: impl Future<Output = anyhow::Result<Rocksdb>> + Unpin,
    mut latest_stored_slot: SlotIndexValue,
    mut geyser_update_rx: mpsc::Receiver<GeyserMessage>,
    reader: Reader,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut disk_buffer = match init_buffer_path.clone() {
        Some(path) => Some(DiskBuffer::new(path).await?),
        None => None,
    };
    let mut messages = VecDeque::new();
    let mut slots = BTreeMap::<Slot, Block>::default();

    // Buffer messages while DB initializes
    let db = loop {
        tokio::select! {
            db_result = &mut db_ready_fut => break db_result?,
            msg = geyser_update_rx.recv() => match msg {
                Some(msg) => {
                    if let Some(buf) = &mut disk_buffer {
                        buf.push(&msg).await?;
                    } else {
                        messages.push_back(msg);
                    }
                }
                None => {
                    anyhow::ensure!(shutdown.is_cancelled(), "failed to get message from update channel");
                    return Ok(());
                }
            },
            () = shutdown.cancelled() => return Ok(()),
        }
    };

    // Replay buffered messages
    let mut disk_reader = match disk_buffer.take() {
        Some(buf) if buf.count > 0 => {
            info!(count = buf.count, "replaying messages from disk buffer");
            Some(buf.into_reader().await?)
        }
        _ => None,
    };

    // Process messages: replay from disk first, then switch to live channel
    while !shutdown.is_cancelled() {
        if disk_reader.is_some() || !messages.is_empty() {
            let ts = Instant::now();
            while ts.elapsed() < Duration::from_millis(400) {
                let msg = if let Some(reader) = &mut disk_reader {
                    match reader.next_message().await {
                        Some(result) => Some(result?),
                        None => {
                            info!("disk buffer replay complete");
                            disk_reader = None;
                            messages.pop_front()
                        }
                    }
                } else {
                    messages.pop_front()
                };
                match msg {
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
