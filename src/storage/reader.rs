use {
    crate::storage::rocksdb::Rocksdb,
    ahash::HashMap,
    solana_commitment_config::CommitmentLevel,
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::{
        sync::{Arc, Mutex, mpsc},
        thread,
        time::{Duration, Instant},
    },
    tokio::sync::{broadcast, oneshot},
};

#[derive(Debug)]
enum ReadRequest {
    Account {
        deadline: Instant,
        x_subscription_id: Arc<str>,
        pubkey: Pubkey,
        commitment: CommitmentLevel,
        tx: oneshot::Sender<ReadResultAccount>,
    },
    Slot {
        deadline: Instant,
        x_subscription_id: Arc<str>,
        commitment: CommitmentLevel,
        tx: oneshot::Sender<ReadResultSlot>,
    },
}

#[derive(Debug)]
pub enum ReadResultAccount {
    ChanClosed,
    Timeout,
    // TODO
}

#[derive(Debug)]
pub enum ReadResultSlot {
    ChanClosed,
    Timeout,
    Slot(Slot),
}

#[derive(Debug, Default)]
pub struct ReaderState {
    pub processed_slot: Slot,
    pub processed_map: HashMap<Pubkey, Arc<Account>>,
    pub confirmed_slot: Slot,
    pub confirmed_map: HashMap<Pubkey, Arc<Account>>,
    pub finalized_slot: Slot,
}

#[derive(Debug, Clone)]
pub struct Reader {
    update_tx: broadcast::Sender<Arc<ReaderState>>,
    req_tx: mpsc::SyncSender<ReadRequest>,
    read_timeout: Duration,
}

impl Reader {
    pub fn new(
        db: Rocksdb,
        req_channel_capacity: usize,
        read_workers: usize,
        read_timeout: Duration,
    ) -> anyhow::Result<(Self, Vec<thread::JoinHandle<anyhow::Result<()>>>)> {
        let (req_tx, req_rx) = mpsc::sync_channel(req_channel_capacity);
        let req_rx = Arc::new(Mutex::new(req_rx));
        let update_tx = broadcast::Sender::new(256); // should be more than enough

        let threads = (0..read_workers)
            .map(|id| {
                let db = db.clone();
                let update_rx = update_tx.subscribe();
                let req_rx = Arc::clone(&req_rx);
                thread::Builder::new()
                    .name(format!("socReader{id:02}"))
                    .spawn(move || Self::spawn_worker(db, update_rx, req_rx))
            })
            .collect::<Result<_, _>>()?;

        let reader = Self {
            req_tx,
            read_timeout,
            update_tx,
        };
        Ok((reader, threads))
    }

    fn spawn_worker(
        db: Rocksdb,
        update_rx: broadcast::Receiver<Arc<ReaderState>>,
        req_rx: Arc<Mutex<mpsc::Receiver<ReadRequest>>>,
    ) -> anyhow::Result<()> {
        std::thread::sleep(std::time::Duration::from_secs(100000));
        Ok(())
    }

    pub fn update(&self, update: Arc<ReaderState>) -> anyhow::Result<()> {
        self.update_tx.send(update)?;
        Ok(())
    }

    pub async fn get_account(
        &self,
        x_subscription_id: Arc<str>,
        pubkey: Pubkey,
        commitment: CommitmentLevel,
    ) -> ReadResultAccount {
        todo!()
    }

    pub async fn get_slot(
        &self,
        x_subscription_id: Arc<str>,
        commitment: CommitmentLevel,
    ) -> ReadResultSlot {
        todo!()
    }
}
