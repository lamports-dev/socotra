use {
    crate::{
        metrics::READ_REQUESTS_TOTAL,
        storage::rocksdb::{GetAccountsError, Rocksdb},
    },
    ahash::HashMap,
    metrics::counter,
    richat_shared::mutex_lock,
    solana_account_decoder::parse_account_data::AccountAdditionalDataV3,
    solana_commitment_config::CommitmentLevel,
    solana_rpc_client::api::config::RpcSimulateTransactionAccountsConfig,
    solana_sdk::{account::Account, clock::Slot, hash::Hash, pubkey::Pubkey},
    solana_transaction::versioned::VersionedTransaction,
    std::{
        fmt,
        sync::{Arc, Mutex, mpsc},
        thread,
        time::{Duration, Instant},
    },
    tokio::sync::{broadcast, oneshot},
    tokio_util::sync::CancellationToken,
    tracing::{Span, info_span},
};

#[derive(Debug)]
enum ReadRequest {
    Slot {
        parent: Span,
        deadline: Instant,
        x_subscription_id: Arc<str>,
        commitment: CommitmentLevel,
        min_context_slot: Option<Slot>,
        tx: oneshot::Sender<ReadResultSlot>,
    },
    Account {
        parent: Span,
        deadline: Instant,
        x_subscription_id: Arc<str>,
        pubkeys: Vec<Pubkey>,
        commitment: CommitmentLevel,
        min_context_slot: Option<Slot>,
        json_parsed: bool,
        tx: oneshot::Sender<ReadResultAccount>,
    },
    SimulateTransaction {
        parent: Span,
        deadline: Instant,
        x_subscription_id: Arc<str>,
        unsanitized_tx: VersionedTransaction,
        sig_verify: bool,
        replace_recent_blockhash: bool,
        config_accounts: Option<RpcSimulateTransactionAccountsConfig>,
        enable_cpi_recording: bool,
        commitment: CommitmentLevel,
        min_context_slot: Option<Slot>,
        tx: oneshot::Sender<ReadResultSimulateTransaction>,
    },
}

pub enum ReadResultSlot {
    ReqChanClosed,
    ReqChanFull,
    ReqDrop,
    Timeout,
    MinContextSlotNotReached { context_slot: Slot },
    Slot(Slot),
}

impl fmt::Debug for ReadResultSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReqChanClosed => write!(f, "ReqChanClosed"),
            Self::ReqChanFull => write!(f, "ReqChanFull"),
            Self::ReqDrop => write!(f, "ReqDrop"),
            Self::Timeout => write!(f, "Timeout"),
            Self::MinContextSlotNotReached { .. } => write!(f, "MinContextSlotNotReached"),
            Self::Slot(_) => write!(f, "Slot"),
        }
    }
}

pub enum ReadResultAccount {
    ReqChanClosed,
    ReqChanFull,
    ReqDrop,
    Timeout,
    MinContextSlotNotReached {
        context_slot: Slot,
    },
    TokenMintUnpackFailed,
    RequestFailed(String),
    Accounts {
        slot: Slot,
        pubkeys: Vec<Pubkey>,
        accounts: Vec<Option<Arc<Account>>>,
        mints: HashMap<Pubkey, AccountAdditionalDataV3>,
    },
}

impl fmt::Debug for ReadResultAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReqChanClosed => write!(f, "ReqChanClosed"),
            Self::ReqChanFull => write!(f, "ReqChanFull"),
            Self::ReqDrop => write!(f, "ReqDrop"),
            Self::Timeout => write!(f, "Timeout"),
            Self::MinContextSlotNotReached { .. } => write!(f, "MinContextSlotNotReached"),
            Self::TokenMintUnpackFailed => write!(f, "TokenMintUnpackFailed"),
            Self::RequestFailed(_) => write!(f, "RequestFailed"),
            Self::Accounts { .. } => write!(f, "Accounts"),
        }
    }
}

impl From<GetAccountsError> for ReadResultAccount {
    fn from(value: GetAccountsError) -> Self {
        if matches!(value, GetAccountsError::TokenMintUnpackFailed) {
            Self::TokenMintUnpackFailed
        } else {
            Self::RequestFailed(value.to_string())
        }
    }
}

pub enum ReadResultSimulateTransaction {
    ReqChanClosed,
    ReqChanFull,
    ReqDrop,
    Timeout,
    MinContextSlotNotReached { context_slot: Slot },
}

impl fmt::Debug for ReadResultSimulateTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReqChanClosed => write!(f, "ReqChanClosed"),
            Self::ReqChanFull => write!(f, "ReqChanFull"),
            Self::ReqDrop => write!(f, "ReqDrop"),
            Self::Timeout => write!(f, "Timeout"),
            Self::MinContextSlotNotReached { .. } => write!(f, "MinContextSlotNotReached"),
        }
    }
}

#[derive(Debug, Default)]
pub struct ReaderState {
    pub processed_slot: Slot,
    pub processed_height: Slot,
    pub processed_map: HashMap<Pubkey, Arc<Account>>,
    pub confirmed_slot: Slot,
    pub confirmed_map: HashMap<Pubkey, Arc<Account>>,
    pub finalized_slot: Slot,
    pub blockhash_map: HashMap<Hash, Slot>,
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

            let loop_deadline = Instant::now() + Duration::from_millis(3);
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
                        ReadRequest::Slot {
                            parent,
                            deadline,
                            x_subscription_id,
                            commitment,
                            min_context_slot,
                            tx,
                        },
                    ) => {
                        let _guard = info_span!(parent: parent, "read_worker").entered();
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
                    (
                        Some(state),
                        ReadRequest::Account {
                            parent,
                            deadline,
                            x_subscription_id,
                            pubkeys,
                            commitment,
                            min_context_slot,
                            json_parsed,
                            tx,
                        },
                    ) => {
                        let _guard = info_span!(parent: parent, "read_worker").entered();
                        let _ = tx.send(if deadline < started_at {
                            ReadResultAccount::Timeout
                        } else {
                            counter!(
                                READ_REQUESTS_TOTAL,
                                "x_subscription_id" => Arc::clone(&x_subscription_id),
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
                                Self::worker_read_accounts(
                                    &db,
                                    state,
                                    pubkeys,
                                    commitment,
                                    slot,
                                    json_parsed,
                                    x_subscription_id,
                                )
                            }
                        });
                    }
                    (
                        Some(state),
                        ReadRequest::SimulateTransaction {
                            parent,
                            deadline,
                            x_subscription_id,
                            unsanitized_tx,
                            sig_verify,
                            replace_recent_blockhash,
                            config_accounts,
                            enable_cpi_recording,
                            commitment,
                            min_context_slot,
                            tx,
                        },
                    ) => {
                        let _guard = info_span!(parent: parent, "read_worker").entered();
                        let _ = tx.send(if deadline < started_at {
                            ReadResultSimulateTransaction::Timeout
                        } else {
                            counter!(
                                READ_REQUESTS_TOTAL,
                                "x_subscription_id" => Arc::clone(&x_subscription_id),
                                "type" => "simulateTransaction"
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
                                ReadResultSimulateTransaction::MinContextSlotNotReached {
                                    context_slot: slot,
                                }
                            } else {
                                Self::worker_simulate_transaction(
                                    &db,
                                    state,
                                    unsanitized_tx,
                                    sig_verify,
                                    replace_recent_blockhash,
                                    config_accounts,
                                    enable_cpi_recording,
                                    commitment,
                                    slot,
                                    x_subscription_id,
                                )
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

    #[inline]
    fn worker_read_accounts(
        db: &Rocksdb,
        state: &ReaderState,
        pubkeys: Vec<Pubkey>,
        commitment: CommitmentLevel,
        slot: Slot,
        json_parsed: bool,
        x_subscription_id: Arc<str>,
    ) -> ReadResultAccount {
        let mut accounts: Vec<Option<Arc<Account>>> = vec![None; pubkeys.len()];
        let mut mints = HashMap::default();

        match db.get_accounts(
            &pubkeys,
            &mut accounts,
            json_parsed,
            &mut mints,
            |pubkey| {
                if commitment == CommitmentLevel::Processed
                    && let Some(account) = state.processed_map.get(pubkey)
                {
                    return Some(Arc::clone(account));
                }
                if matches!(
                    commitment,
                    CommitmentLevel::Processed | CommitmentLevel::Confirmed
                ) && let Some(account) = state.confirmed_map.get(pubkey)
                {
                    return Some(Arc::clone(account));
                }
                None
            },
            x_subscription_id,
        ) {
            Ok(db_slot) => ReadResultAccount::Accounts {
                slot: if commitment == CommitmentLevel::Finalized {
                    db_slot
                } else {
                    slot
                },
                pubkeys,
                accounts,
                mints,
            },
            Err(error) => error.into(),
        }
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn worker_simulate_transaction(
        db: &Rocksdb,
        state: &ReaderState,
        unsanitized_tx: VersionedTransaction,
        sig_verify: bool,
        replace_recent_blockhash: bool,
        config_accounts: Option<RpcSimulateTransactionAccountsConfig>,
        enable_cpi_recording: bool,
        commitment: CommitmentLevel,
        slot: Slot,
        x_subscription_id: Arc<str>,
    ) -> ReadResultSimulateTransaction {
        todo!()
    }

    pub fn update(&self, update: Arc<ReaderState>) -> anyhow::Result<()> {
        self.update_tx.send(update)?;
        Ok(())
    }

    pub async fn get_slot(
        &self,
        x_subscription_id: Arc<str>,
        commitment: CommitmentLevel,
        min_context_slot: Option<Slot>,
    ) -> ReadResultSlot {
        let (tx, rx) = oneshot::channel();
        match self.req_tx.try_send(ReadRequest::Slot {
            parent: Span::current(),
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

    pub async fn get_accounts(
        &self,
        x_subscription_id: Arc<str>,
        pubkeys: Vec<Pubkey>,
        commitment: CommitmentLevel,
        min_context_slot: Option<Slot>,
        json_parsed: bool,
    ) -> ReadResultAccount {
        let (tx, rx) = oneshot::channel();
        match self.req_tx.try_send(ReadRequest::Account {
            parent: Span::current(),
            deadline: Instant::now() + self.read_timeout,
            x_subscription_id,
            pubkeys,
            commitment,
            min_context_slot,
            json_parsed,
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

    #[allow(clippy::too_many_arguments)]
    pub async fn simulate_transaction(
        &self,
        x_subscription_id: Arc<str>,
        unsanitized_tx: VersionedTransaction,
        sig_verify: bool,
        replace_recent_blockhash: bool,
        config_accounts: Option<RpcSimulateTransactionAccountsConfig>,
        enable_cpi_recording: bool,
        commitment: CommitmentLevel,
        min_context_slot: Option<Slot>,
    ) -> ReadResultSimulateTransaction {
        let (tx, rx) = oneshot::channel();
        match self.req_tx.try_send(ReadRequest::SimulateTransaction {
            parent: Span::current(),
            deadline: Instant::now() + self.read_timeout,
            x_subscription_id,
            unsanitized_tx,
            sig_verify,
            replace_recent_blockhash,
            config_accounts,
            enable_cpi_recording,
            commitment,
            min_context_slot,
            tx,
        }) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Disconnected(_)) => {
                return ReadResultSimulateTransaction::ReqChanClosed;
            }
            Err(mpsc::TrySendError::Full(_)) => return ReadResultSimulateTransaction::ReqChanFull,
        };

        match rx.await {
            Ok(value) => value,
            Err(_) => ReadResultSimulateTransaction::ReqDrop,
        }
    }
}
