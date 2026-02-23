use {
    crate::{metrics::READ_REQUESTS_TOTAL, storage::rocksdb::Rocksdb},
    ahash::HashMap,
    metrics::counter,
    richat_shared::mutex_lock,
    solana_commitment_config::CommitmentLevel,
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::{
        sync::{Arc, Mutex, mpsc},
        thread,
        time::{Duration, Instant},
    },
    tokio::sync::{broadcast, oneshot},
    tokio_util::sync::CancellationToken,
};

#[derive(Debug)]
enum ReadRequest {
    Account {
        deadline: Instant,
        x_subscription_id: Arc<str>,
        pubkeys: Vec<Pubkey>,
        commitment: CommitmentLevel,
        min_context_slot: Option<Slot>,
        tx: oneshot::Sender<ReadResultAccount>,
    },
    Slot {
        deadline: Instant,
        x_subscription_id: Arc<str>,
        commitment: CommitmentLevel,
        min_context_slot: Option<Slot>,
        tx: oneshot::Sender<ReadResultSlot>,
    },
}

#[derive(Debug)]
pub enum ReadResultAccount {
    ReqChanClosed,
    ReqChanFull,
    ReqDrop,
    Timeout,
    MinContextSlotNotReached { context_slot: Slot },
    // TODO
}

#[derive(Debug)]
pub enum ReadResultSlot {
    ReqChanClosed,
    ReqChanFull,
    ReqDrop,
    Timeout,
    MinContextSlotNotReached { context_slot: Slot },
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
        shutdown: CancellationToken,
    ) -> anyhow::Result<(Self, Vec<thread::JoinHandle<anyhow::Result<()>>>)> {
        let (req_tx, req_rx) = mpsc::sync_channel(req_channel_capacity);
        let req_rx = Arc::new(Mutex::new(req_rx));
        let update_tx = broadcast::Sender::new(256); // should be more than enough

        let threads = (0..read_workers)
            .map(|id| {
                let db = db.clone();
                let update_rx = update_tx.subscribe();
                let req_rx = Arc::clone(&req_rx);
                let shutdown = shutdown.clone();
                thread::Builder::new()
                    .name(format!("socReader{id:02}"))
                    .spawn(move || Self::spawn_worker(db, update_rx, req_rx, shutdown))
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
        mut update_rx: broadcast::Receiver<Arc<ReaderState>>,
        req_rx: Arc<Mutex<mpsc::Receiver<ReadRequest>>>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut state = None;
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }

            let mut is_empty_update = true;
            let mut is_empty_req = true;

            loop {
                match update_rx.try_recv() {
                    Ok(new_state) => {
                        state = Some(new_state);
                        is_empty_update = false;
                    }
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(broadcast::error::TryRecvError::Closed) => return Ok(()),
                    Err(broadcast::error::TryRecvError::Lagged(_)) => {
                        anyhow::bail!("lagged reader")
                    }
                }
            }

            let loop_deadline = Instant::now() + Duration::from_millis(5);
            loop {
                let request = {
                    match mutex_lock(&req_rx).try_recv() {
                        Ok(request) => request,
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
                    }
                };
                is_empty_req = false;

                let started_at = Instant::now();
                match (&state, request) {
                    (
                        Some(state),
                        ReadRequest::Account {
                            deadline,
                            x_subscription_id,
                            pubkeys,
                            commitment,
                            min_context_slot,
                            tx,
                        },
                    ) => {
                        let _ = tx.send(if deadline < started_at {
                            ReadResultAccount::Timeout
                        } else {
                            counter!(
                                READ_REQUESTS_TOTAL,
                                "x_subscription_id" => x_subscription_id,
                                "type" => "account"
                            )
                            .increment(pubkeys.len() as u64);

                            let slot = match commitment {
                                CommitmentLevel::Processed => state.processed_slot,
                                CommitmentLevel::Confirmed => state.confirmed_slot,
                                CommitmentLevel::Finalized => state.finalized_slot,
                            };

                            if let Some(min_context_slot) = min_context_slot
                                && slot < min_context_slot
                            {
                                ReadResultAccount::MinContextSlotNotReached { context_slot: slot }
                            } else {
                                todo!()
                            }
                        });
                    }
                    (
                        Some(state),
                        ReadRequest::Slot {
                            deadline,
                            x_subscription_id,
                            commitment,
                            min_context_slot,
                            tx,
                        },
                    ) => {
                        let _ = tx.send(if deadline < started_at {
                            ReadResultSlot::Timeout
                        } else {
                            counter!(
                                READ_REQUESTS_TOTAL,
                                "x_subscription_id" => x_subscription_id,
                                "type" => "slot"
                            )
                            .increment(1);

                            let slot = match commitment {
                                CommitmentLevel::Processed => state.processed_slot,
                                CommitmentLevel::Confirmed => state.confirmed_slot,
                                CommitmentLevel::Finalized => state.finalized_slot,
                            };

                            if let Some(min_context_slot) = min_context_slot
                                && slot < min_context_slot
                            {
                                ReadResultSlot::MinContextSlotNotReached { context_slot: slot }
                            } else {
                                ReadResultSlot::Slot(slot)
                            }
                        });
                    }
                    (None, _) => {}
                }

                if Instant::now() >= loop_deadline {
                    break;
                }
            }

            if is_empty_update && is_empty_req {
                thread::sleep(Duration::from_millis(1));
            }
        }
    }

    pub fn update(&self, update: Arc<ReaderState>) -> anyhow::Result<()> {
        self.update_tx.send(update)?;
        Ok(())
    }

    pub async fn get_account(
        &self,
        x_subscription_id: Arc<str>,
        pubkeys: Vec<Pubkey>,
        commitment: CommitmentLevel,
        min_context_slot: Option<Slot>,
    ) -> ReadResultAccount {
        let (tx, rx) = oneshot::channel();
        match self.req_tx.try_send(ReadRequest::Account {
            deadline: Instant::now() + self.read_timeout,
            x_subscription_id,
            pubkeys,
            commitment,
            min_context_slot,
            tx,
        }) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Disconnected(_)) => return ReadResultAccount::ReqChanClosed,
            Err(mpsc::TrySendError::Full(_)) => return ReadResultAccount::ReqChanFull,
        };

        match rx.await {
            Ok(value) => value,
            Err(_) => ReadResultAccount::ReqDrop,
        }
    }

    pub async fn get_slot(
        &self,
        x_subscription_id: Arc<str>,
        commitment: CommitmentLevel,
        min_context_slot: Option<Slot>,
    ) -> ReadResultSlot {
        let (tx, rx) = oneshot::channel();
        match self.req_tx.try_send(ReadRequest::Slot {
            deadline: Instant::now() + self.read_timeout,
            x_subscription_id,
            commitment,
            min_context_slot,
            tx,
        }) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Disconnected(_)) => return ReadResultSlot::ReqChanClosed,
            Err(mpsc::TrySendError::Full(_)) => return ReadResultSlot::ReqChanFull,
        };

        match rx.await {
            Ok(value) => value,
            Err(_) => ReadResultSlot::ReqDrop,
        }
    }
}
