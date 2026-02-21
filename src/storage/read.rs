use {
    crate::storage::rocksdb::Rocksdb,
    ahash::HashMap,
    solana_commitment_config::CommitmentLevel,
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::{
        sync::{Arc, mpsc},
        time::Instant,
    },
    tokio::sync::{broadcast, oneshot},
};

#[derive(Debug)]
pub enum ReadRequest {
    Account {
        deadline: Instant,
        x_subscription_id: Arc<str>,
        tx: oneshot::Sender<ReadResultAccount>,
        pubkey: Pubkey,
        commitment: CommitmentLevel,
    },
    Slot {
        deadline: Instant,
        x_subscription_id: Arc<str>,
        tx: oneshot::Sender<ReadResultSlot>,
        commitment: CommitmentLevel,
    },
}

#[derive(Debug)]
pub enum ReadResultAccount {
    Timeout,
    //
}

#[derive(Debug)]
pub enum ReadResultSlot {
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

#[derive(Debug)]
pub struct Reader {
    db: Rocksdb,
}

impl Reader {
    pub fn new(
        db: Rocksdb,
        update_rx: broadcast::Receiver<Arc<ReaderState>>,
        req_rx: mpsc::Receiver<ReadRequest>,
        read_workers: usize,
    ) -> Self {
        todo!()
    }
}
