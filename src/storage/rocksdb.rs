use {
    crate::{
        config::ConfigStorageRocksdbCompression,
        metrics::{READ_ACCOUNTS_BYTES_TOTAL, READ_ACCOUNTS_SECONDS_TOTAL, READ_ACCOUNTS_TOTAL},
        storage::reader::ReaderState,
    },
    ahash::HashMap,
    anyhow::Context,
    bytes::Buf,
    metrics::{counter, gauge},
    prost::encoding::{decode_varint, encode_varint},
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, IngestExternalFileOptions,
        Options, WriteBatch,
    },
    solana_account_decoder::{
        parse_account_data::{AccountAdditionalDataV3, SplTokenAdditionalDataV2},
        parse_token::{get_token_account_mint, is_known_spl_token_id},
    },
    solana_address_lookup_table_interface::{
        error::AddressLookupError, program as address_lookup_table_program,
        state::AddressLookupTable,
    },
    solana_commitment_config::CommitmentLevel,
    solana_rpc_client_types::{
        config::RpcSimulateTransactionAccountsConfig, response::RpcBlockhash,
    },
    solana_runtime::bank::TransactionSimulationResult,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_sdk::{
        account::Account,
        clock::{MAX_PROCESSING_AGE, Slot, UnixTimestamp},
        hash::Hash,
        message::{AddressLoader, v0::LoadedAddresses},
        pubkey::Pubkey,
        slot_hashes::SlotHashes,
        sysvar,
    },
    solana_transaction::{sanitized::MessageHash, versioned::VersionedTransaction},
    solana_transaction_error::AddressLoaderError,
    spl_token_2022_interface::{
        extension::{
            BaseStateWithExtensions, StateWithExtensions,
            interest_bearing_mint::InterestBearingConfig, scaled_ui_amount::ScaledUiAmountConfig,
        },
        state::Mint,
    },
    std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
        rc::Rc,
        sync::Arc,
        time::Instant,
    },
    tracing::{Span, info, info_span, instrument},
};

pub trait ColumnName {
    const NAME: &'static str;
}

#[derive(Debug)]
struct SlotIndexKey;

impl ColumnName for SlotIndexKey {
    const NAME: &'static str = "slot_index";
}

#[derive(Debug, Clone, Copy)]
pub struct SlotIndexValue {
    pub slot: Slot,
    pub height: Slot,
}

impl SlotIndexValue {
    fn encode(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&self.slot.to_be_bytes());
        buf[8..].copy_from_slice(&self.height.to_be_bytes());
        buf
    }

    fn decode(slice: &[u8]) -> anyhow::Result<Self> {
        let bytes: [u8; 16] = slice.try_into().context("invalid slot index data length")?;
        Ok(Self {
            slot: Slot::from_be_bytes(
                bytes[..8]
                    .try_into()
                    .expect("failed to get slot bytes from slice"),
            ),
            height: Slot::from_be_bytes(
                bytes[8..]
                    .try_into()
                    .expect("failed to get height bytes from slice"),
            ),
        })
    }
}

#[derive(Debug)]
pub struct AccountIndexKey;

impl ColumnName for AccountIndexKey {
    const NAME: &'static str = "account_index";
}

impl AccountIndexKey {
    pub const fn encode(pubkey: &Pubkey) -> [u8; 32] {
        pubkey.to_bytes()
    }
}

pub struct AccountIndexValue;

impl AccountIndexValue {
    pub fn encode(account: &Account, buf: &mut Vec<u8>) {
        encode_varint(account.lamports, buf);
        encode_varint(account.data.len() as u64, buf);
        buf.extend_from_slice(&account.data);
        buf.extend_from_slice(account.owner.as_ref());
        buf.push(if account.executable { 1 } else { 0 });
        encode_varint(account.rent_epoch, buf);
    }

    fn decode(mut data: &[u8]) -> Result<Account, prost::DecodeError> {
        let lamports = decode_varint(&mut data)?;
        let data_len = decode_varint(&mut data)? as usize;
        if data.remaining() < data_len {
            return Err(
                #[allow(deprecated)]
                {
                    prost::DecodeError::new("not enough data for account data")
                },
            );
        }
        let account_data = data[..data_len].to_vec();
        data.advance(data_len);
        if data.remaining() < 33 {
            return Err(
                #[allow(deprecated)]
                {
                    prost::DecodeError::new("not enough data for owner and executable")
                },
            );
        }
        let owner = Pubkey::from(<[u8; 32]>::try_from(&data[..32]).unwrap());
        data.advance(32);
        let executable = data[0] != 0;
        data.advance(1);
        let rent_epoch = decode_varint(&mut data)?;
        Ok(Account {
            lamports,
            data: account_data,
            owner,
            executable,
            rent_epoch,
        })
    }
}

#[derive(Debug)]
pub struct BlockhashIndexKey;

impl ColumnName for BlockhashIndexKey {
    const NAME: &'static str = "blockhash_index";
}

#[derive(Debug, thiserror::Error)]
pub enum GetAccountsError {
    #[error("slot index: {0}")]
    SlotIndex(#[from] GetSlotIndexError),
    #[error("account reader: {0}")]
    AccountReader(#[from] AccountReaderError),
    #[error("Invalid param: Token mint could not be unpacked")]
    TokenMintUnpackFailed,
}

#[derive(Debug)]
pub struct GetSimulateTransactionData {
    db_slot: Slot,
}

#[derive(Debug, thiserror::Error)]
pub enum GetSimulateTransactionDataError {
    #[error("slot index: {0}")]
    SlotIndex(#[from] GetSlotIndexError),
    #[error("account reader: {0}")]
    AccountReader(#[from] AccountReaderError),
    #[error("blockhash not found")]
    BlockhashNotFound,
    #[error("{0}")]
    InvalidParams(String),
}

#[derive(Debug, Clone)]
pub struct Rocksdb {
    db: Arc<DB>,
    path: PathBuf,
    accounts_compression: DBCompressionType,
}

impl Rocksdb {
    pub fn open(
        path: PathBuf,
        compression: ConfigStorageRocksdbCompression,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("failed to create db directory: {:?}", path))?;

        let accounts_compression = compression.into();

        let db_options = Self::get_db_options();
        let cf_descriptors = Self::cf_descriptors(accounts_compression);

        let db = Arc::new(
            DB::open_cf_descriptors(&db_options, &path, cf_descriptors)
                .with_context(|| format!("failed to open rocksdb with path: {:?}", path))?,
        );

        Ok(Self {
            db,
            path,
            accounts_compression,
        })
    }

    fn get_db_options() -> Options {
        let mut options = Options::default();

        // Create if not exists
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Set_max_background_jobs(N), configures N/4 low priority threads and 3N/4 high priority threads
        options.set_max_background_jobs(num_cpus::get() as i32);

        // Set max total WAL size to 4GiB
        options.set_max_total_wal_size(4 * 1024 * 1024 * 1024);

        options
    }

    fn cf_descriptors(compression: DBCompressionType) -> Vec<ColumnFamilyDescriptor> {
        vec![
            Self::cf_descriptor::<SlotIndexKey>(DBCompressionType::None),
            Self::cf_descriptor::<AccountIndexKey>(compression),
            Self::cf_descriptor::<BlockhashIndexKey>(DBCompressionType::None),
        ]
    }

    fn cf_descriptor<C: ColumnName>(compression: DBCompressionType) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options(None, compression))
    }

    fn get_cf_options(options: Option<Options>, compression: DBCompressionType) -> Options {
        let mut options = options.unwrap_or_default();

        const MAX_WRITE_BUFFER_SIZE: u64 = 512 * 1024 * 1024;
        options.set_max_write_buffer_number(8);
        options.set_write_buffer_size(MAX_WRITE_BUFFER_SIZE as usize);

        let file_num_compaction_trigger = 4;
        let total_size_base = MAX_WRITE_BUFFER_SIZE * file_num_compaction_trigger;
        let file_size_base = total_size_base / 10;
        options.set_level_zero_file_num_compaction_trigger(file_num_compaction_trigger as i32);
        options.set_max_bytes_for_level_base(total_size_base);
        options.set_target_file_size_base(file_size_base);

        options.set_compression_type(compression);

        options.set_writable_file_max_buffer_size(4 * 1024 * 1024);

        options
    }

    fn cf_handle<C: ColumnName>(&self) -> &ColumnFamily {
        self.db
            .cf_handle(C::NAME)
            .expect("should never get an unknown column")
    }

    pub fn sst_config(&self, segment: u16) -> (PathBuf, Options) {
        let path = self.path.join(format!("{segment:04}.sst"));
        // let options = Self::get_db_options();
        // Self::get_cf_options(Some(options), compression)
        let options = Self::get_cf_options(None, self.accounts_compression);
        (path, options)
    }

    pub fn sst_ingest<P>(&self, files: Vec<P>, slot_info: SlotIndexValue) -> anyhow::Result<()>
    where
        P: AsRef<Path>,
    {
        let ts = Instant::now();
        let mut ingest_options = IngestExternalFileOptions::default();
        ingest_options.set_move_files(true);
        ingest_options.set_snapshot_consistency(false);
        ingest_options.set_allow_global_seqno(false);
        ingest_options.set_allow_blocking_flush(false);
        self.db
            .ingest_external_file_cf_opts(
                self.cf_handle::<AccountIndexKey>(),
                &ingest_options,
                files,
            )
            .context("failed to ingest SST files")?;
        info!(elapsed = ?ts.elapsed(), "db created from sst files");

        self.store_slot_info(slot_info)?;

        Ok(())
    }

    pub fn sst_ingest_files<P>(&self, files: Vec<P>) -> anyhow::Result<()>
    where
        P: AsRef<Path>,
    {
        let ts = Instant::now();
        let mut ingest_options = IngestExternalFileOptions::default();
        ingest_options.set_move_files(true);
        ingest_options.set_snapshot_consistency(false);
        ingest_options.set_allow_global_seqno(false);
        ingest_options.set_allow_blocking_flush(false);
        self.db
            .ingest_external_file_cf_opts(
                self.cf_handle::<AccountIndexKey>(),
                &ingest_options,
                files,
            )
            .context("failed to ingest SST files")?;
        info!(elapsed = ?ts.elapsed(), "db created from sst files");
        Ok(())
    }

    pub fn destroy(self) {
        if let Some(db) = Arc::into_inner(self.db) {
            drop(db);
            let _ = DB::destroy(&Options::default(), &self.path);
        }
    }

    pub fn store_slot_info(&self, slot_info: SlotIndexValue) -> anyhow::Result<()> {
        self.db
            .put_cf(self.cf_handle::<SlotIndexKey>(), "slot", slot_info.encode())
            .context("failed to store slot value")
    }

    pub fn get_state_slot_info(&self) -> anyhow::Result<Option<SlotIndexValue>> {
        self.db
            .get_cf(self.cf_handle::<SlotIndexKey>(), "slot")
            .context("failed to get slot data")?
            .map(|data| SlotIndexValue::decode(&data))
            .transpose()
    }

    pub fn load_blockhashes(&self) -> anyhow::Result<BTreeMap<Slot, Hash>> {
        let cf = self.cf_handle::<BlockhashIndexKey>();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        let mut map = BTreeMap::new();
        for item in iter {
            let (key, value) = item.context("failed to iterate blockhash_index")?;
            let height = Slot::from_be_bytes(
                key.as_ref()
                    .try_into()
                    .context("invalid blockhash_index key length")?,
            );
            let bytes: [u8; 32] = value
                .as_ref()
                .try_into()
                .context("invalid blockhash_index value length")?;
            map.insert(height, Hash::new_from_array(bytes));
        }
        Ok(map)
    }

    #[instrument(skip_all, fields(slot = state_slot_info.slot, accounts))]
    pub fn store_new_state(
        &self,
        state_slot_info: SlotIndexValue,
        accounts: impl Iterator<Item = (Pubkey, Arc<Account>)>,
        blockhash: Hash,
    ) -> anyhow::Result<()> {
        let span = info_span!("generate_batch").entered();
        let mut batch = WriteBatch::with_capacity_bytes(256 * 1024 * 1024); // 256MiB

        batch.put_cf(
            self.cf_handle::<SlotIndexKey>(),
            "slot",
            state_slot_info.encode(),
        );

        // Store blockhash for this finalized height
        let bh_cf = self.cf_handle::<BlockhashIndexKey>();
        batch.put_cf(
            bh_cf,
            state_slot_info.height.to_be_bytes(),
            blockhash.to_bytes(),
        );

        // Prune old blockhash entries
        let min_height = state_slot_info
            .height
            .saturating_sub(MAX_PROCESSING_AGE as u64 - 1);
        if min_height > 0 {
            batch.delete_range_cf(bh_cf, 0u64.to_be_bytes(), min_height.to_be_bytes());
        }

        // Store new accounts state
        let acc_cf = self.cf_handle::<AccountIndexKey>();
        let mut num_accounts = 0u64;
        let mut buf = Vec::with_capacity(16 * 1024 * 1024); // 16MiB
        for (pubkey, account) in accounts {
            buf.clear();
            AccountIndexValue::encode(&account, &mut buf);
            batch.put_cf(acc_cf, AccountIndexKey::encode(&pubkey), &buf);
            num_accounts += 1;
        }
        drop(span);
        Span::current().record("accounts", num_accounts);

        {
            let _span = info_span!("write_batch", size = batch.size_in_bytes()).entered();
            self.db
                .write(batch)
                .context("failed to write accounts in batch")
        }
    }

    #[instrument(skip_all, fields(pubkeys = pubkeys.len()))]
    pub fn get_accounts(
        &self,
        pubkeys: &[Pubkey],
        accounts: &mut [Option<Arc<Account>>],
        json_parsed: bool,
        mints: &mut HashMap<Pubkey, AccountAdditionalDataV3>,
        state: &ReaderState,
        commitment: CommitmentLevel,
        x_subscription_id: Arc<str>,
    ) -> Result<Slot, GetAccountsError> {
        let snapshot = self.db.snapshot();
        let slot = snapshot_load_slot_index(self, &snapshot)?.slot;

        let mut reader = AccountReader::new(self, &snapshot, x_subscription_id);

        let indices: Vec<usize> = pubkeys
            .iter()
            .enumerate()
            .filter_map(|(i, pubkey)| {
                accounts[i] = state.get_account(pubkey, commitment);
                accounts[i].is_none().then_some(i)
            })
            .collect();

        let results = reader.get_multi(indices.iter().map(|&i| &pubkeys[i]));
        for (idx, result) in indices.into_iter().zip(results) {
            if let Some(account) = result? {
                accounts[idx] = Some(Arc::new(account));
            }
        }

        if json_parsed {
            let _span = info_span!("json_parsed").entered();

            let mut mint_pubkeys: Vec<Pubkey> = Vec::new();
            for account in accounts.iter().flatten() {
                if is_known_spl_token_id(&account.owner)
                    && let Some(mint_pubkey) = get_token_account_mint(&account.data)
                    && !mint_pubkeys.contains(&mint_pubkey)
                {
                    mint_pubkeys.push(mint_pubkey);
                }
            }

            if !mint_pubkeys.is_empty() {
                let clock_id = sysvar::clock::id();
                let clock_account = reader.get_sysvar(&clock_id, state, commitment)?;
                // Clock layout: slot(8) + epoch_start_timestamp(8) + epoch(8) + leader_schedule_epoch(8) + unix_timestamp(8)
                let unix_timestamp = clock_account
                    .data
                    .get(32..40)
                    .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
                    .unwrap_or(0);

                let mut mint_accounts: Vec<Option<Arc<Account>>> = vec![None; mint_pubkeys.len()];
                let db_mint_indices: Vec<usize> = mint_pubkeys
                    .iter()
                    .enumerate()
                    .filter_map(|(i, pubkey)| {
                        mint_accounts[i] = state.get_account(pubkey, commitment);
                        mint_accounts[i].is_none().then_some(i)
                    })
                    .collect();

                let mint_results =
                    reader.get_multi(db_mint_indices.iter().map(|&i| &mint_pubkeys[i]));
                for (idx, result) in db_mint_indices.into_iter().zip(mint_results) {
                    if let Some(account) = result? {
                        mint_accounts[idx] = Some(Arc::new(account));
                    }
                }

                for (mint_pubkey, mint_account) in mint_pubkeys.into_iter().zip(mint_accounts) {
                    if let Some(mint_account) = mint_account {
                        let additional_data =
                            get_additional_mint_data(&mint_account.data, unix_timestamp)?;
                        mints.insert(
                            mint_pubkey,
                            AccountAdditionalDataV3 {
                                spl_token_additional_data: Some(additional_data),
                            },
                        );
                    }
                }
            }
        }

        Ok(slot)
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub fn get_simulate_transaction_data(
        &self,
        state: &ReaderState,
        mut unsanitized_tx: VersionedTransaction,
        sig_verify: bool,
        replace_recent_blockhash: bool,
        config_accounts: Option<RpcSimulateTransactionAccountsConfig>,
        enable_cpi_recording: bool,
        commitment: CommitmentLevel,
        mut slot: Slot,
        x_subscription_id: Arc<str>,
        agave_feature_enable_static_instruction_limit: bool,
    ) -> Result<GetSimulateTransactionData, GetSimulateTransactionDataError> {
        let snapshot = self.db.snapshot();

        let mut replacement_blockhash: Option<RpcBlockhash> = None;
        if replace_recent_blockhash {
            if sig_verify {
                return Err(GetSimulateTransactionDataError::InvalidParams(
                    "sigVerify may not be used with replaceRecentBlockhash".to_owned(),
                ));
            }

            let height = match commitment {
                CommitmentLevel::Processed => state.processed_height,
                CommitmentLevel::Confirmed => state.confirmed_height,
                CommitmentLevel::Finalized => {
                    let slot_info = snapshot_load_slot_index(self, &snapshot)?;
                    slot = slot_info.slot;
                    slot_info.height
                }
            };
            let (&recent_blockhash, _) = state
                .blockhash_map
                .iter()
                .find(|&(_, &h)| h == height)
                .ok_or(GetSimulateTransactionDataError::BlockhashNotFound)?;

            unsanitized_tx
                .message
                .set_recent_blockhash(recent_blockhash);

            let age = state.processed_height.saturating_sub(height);
            let last_valid_block_height = height + MAX_PROCESSING_AGE as u64 - age;
            replacement_blockhash.replace(RpcBlockhash {
                blockhash: recent_blockhash.to_string(),
                last_valid_block_height,
            });
        }

        let mut reader = AccountReader::new(self, &snapshot, x_subscription_id);
        let address_loader =
            SnapshotAddressLoader::new(&unsanitized_tx, &mut reader, state, commitment, slot)?;

        // sanitize transaction
        let transaction = RuntimeTransaction::try_create(
            unsanitized_tx,
            MessageHash::Compute,
            None,
            address_loader,
            &Default::default(), // reserved_account_keys
            agave_feature_enable_static_instruction_limit,
        )
        .map_err(|err| {
            GetSimulateTransactionDataError::InvalidParams(format!("invalid transaction: {err}"))
        })?;

        let verification_error = if sig_verify {
            transaction.verify().err()
        } else {
            None
        };

        let TransactionSimulationResult {
            result,
            logs,
            post_simulation_accounts,
            units_consumed,
            loaded_accounts_data_size,
            return_data,
            inner_instructions,
            fee,
            pre_balances,
            post_balances,
            pre_token_balances,
            post_token_balances,
        } = if let Some(err) = verification_error {
            TransactionSimulationResult::new_error(err)
        } else {
            todo!()
            // bank.simulate_transaction(&transaction, enable_cpi_recording)
        };

        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GetSlotIndexError {
    #[error("rocksdb: {0}")]
    Rocksdb(#[from] rocksdb::Error),
    #[error("slot not found")]
    NotFound,
    #[error("decode: {0}")]
    Decode(anyhow::Error),
}

fn snapshot_load_slot_index(
    rocksdb: &Rocksdb,
    snapshot: &rocksdb::Snapshot<'_>,
) -> Result<SlotIndexValue, GetSlotIndexError> {
    let slot_data = snapshot
        .get_cf(rocksdb.cf_handle::<SlotIndexKey>(), "slot")?
        .ok_or(GetSlotIndexError::NotFound)?;
    SlotIndexValue::decode(&slot_data).map_err(GetSlotIndexError::Decode)
}

#[derive(Debug, thiserror::Error)]
pub enum AccountReaderError {
    #[error("rocksdb: {0}")]
    Rocksdb(#[from] rocksdb::Error),
    #[error("decode: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("sysvar not found")]
    SysvarNotFound,
}

struct AccountReader<'a> {
    snapshot: &'a rocksdb::Snapshot<'a>,
    cf: &'a ColumnFamily,
    x_subscription_id: Arc<str>,
    seconds: f64,
    accounts_read: usize,
    bytes_read: usize,
}

impl<'a> AccountReader<'a> {
    fn new(
        rocksdb: &'a Rocksdb,
        snapshot: &'a rocksdb::Snapshot<'a>,
        x_subscription_id: Arc<str>,
    ) -> Self {
        Self {
            snapshot,
            cf: rocksdb.cf_handle::<AccountIndexKey>(),
            x_subscription_id,
            seconds: 0.0,
            accounts_read: 0,
            bytes_read: 0,
        }
    }

    /// Load sysvar account based on commitment level.
    ///
    /// Sysvars that update every slot (Clock, SlotHashes, SlotHistory) must come
    /// from the in-memory state at processed/confirmed — falling back to DB would
    /// give stale data. Missing at any level is an error.
    ///
    /// All other sysvars (EpochSchedule, Rent, etc.) update rarely or never, so
    /// they may not be in the in-memory maps. For those we check state first and
    /// fall back to the DB snapshot.
    fn get_sysvar(
        &mut self,
        pubkey: &Pubkey,
        state: &ReaderState,
        commitment: CommitmentLevel,
    ) -> Result<Account, AccountReaderError> {
        let updates_every_slot = *pubkey == sysvar::clock::id()
            || *pubkey == sysvar::slot_hashes::id()
            || *pubkey == sysvar::slot_history::id();

        if updates_every_slot {
            match commitment {
                // Check processed_map first, fall back to confirmed_map
                // only when heights match (otherwise confirmed_map is stale).
                CommitmentLevel::Processed => {
                    let account = state.processed_map.get(pubkey).or_else(|| {
                        (state.processed_height == state.confirmed_height)
                            .then(|| state.confirmed_map.get(pubkey))
                            .flatten()
                    });
                    account
                        .map(|a| (**a).clone())
                        .ok_or(AccountReaderError::SysvarNotFound)
                }
                CommitmentLevel::Confirmed => state
                    .confirmed_map
                    .get(pubkey)
                    .map(|a| (**a).clone())
                    .ok_or(AccountReaderError::SysvarNotFound),
                CommitmentLevel::Finalized => self
                    .get_multi(std::iter::once(pubkey))
                    .next()
                    .unwrap_or(Ok(None))?
                    .ok_or(AccountReaderError::SysvarNotFound),
            }
        } else {
            // Rarely-updated sysvars: check state, fall back to DB.
            match state.get_account(pubkey, commitment) {
                Some(account) => Ok((*account).clone()),
                None => self
                    .get_multi(std::iter::once(pubkey))
                    .next()
                    .unwrap_or(Ok(None))?
                    .ok_or(AccountReaderError::SysvarNotFound),
            }
        }
    }

    fn get_multi<'b>(
        &mut self,
        pubkeys: impl IntoIterator<Item = &'b Pubkey>,
    ) -> impl Iterator<Item = Result<Option<Account>, AccountReaderError>> + '_ {
        let started_at = Instant::now();

        let results = self.snapshot.multi_get_cf(
            pubkeys
                .into_iter()
                .map(|pk| (self.cf, AccountIndexKey::encode(pk))),
        );

        self.seconds += started_at.elapsed().as_secs_f64();
        self.accounts_read += results.len();
        self.bytes_read += results
            .iter()
            .filter_map(|result| Some(result.as_ref().ok()?.as_ref()?.len()))
            .sum::<usize>();

        results.into_iter().map(|result| match result? {
            Some(data) => Ok(Some(AccountIndexValue::decode(&data)?)),
            None => Ok(None),
        })
    }
}

impl Drop for AccountReader<'_> {
    fn drop(&mut self) {
        counter!(READ_ACCOUNTS_TOTAL, "x_subscription_id" => Arc::clone(&self.x_subscription_id))
            .increment(self.accounts_read as u64);
        gauge!(READ_ACCOUNTS_SECONDS_TOTAL, "x_subscription_id" => Arc::clone(&self.x_subscription_id))
            .increment(self.seconds);
        counter!(READ_ACCOUNTS_BYTES_TOTAL, "x_subscription_id" => Arc::clone(&self.x_subscription_id))
            .increment(self.bytes_read as u64);
    }
}

struct SnapshotAddressLoader {
    accounts: Rc<Vec<(Pubkey, Account)>>,
    current_slot: Slot,
    slot_hashes: Rc<SlotHashes>,
}

impl SnapshotAddressLoader {
    fn new(
        unsanitized_tx: &VersionedTransaction,
        reader: &mut AccountReader<'_>,
        state: &ReaderState,
        commitment: CommitmentLevel,
        slot: Slot,
    ) -> Result<Self, AccountReaderError> {
        let lookups = unsanitized_tx
            .message
            .address_table_lookups()
            .unwrap_or_default();
        if lookups.is_empty() {
            return Ok(Self {
                accounts: Rc::new(Vec::new()),
                current_slot: slot,
                slot_hashes: Rc::new(SlotHashes::default()),
            });
        }

        // Load SlotHashes sysvar
        let slot_hashes_id = sysvar::slot_hashes::id();
        let slot_hashes_account = reader.get_sysvar(&slot_hashes_id, state, commitment)?;
        let slot_hashes: SlotHashes =
            bincode::deserialize(&slot_hashes_account.data).unwrap_or_default();

        // Load lookup table accounts
        let mut accounts: Vec<Option<(Pubkey, Account)>> = Vec::with_capacity(lookups.len());
        let mut db_indices = Vec::with_capacity(lookups.len());
        for lookup in lookups {
            if let Some(account) = state.get_account(&lookup.account_key, commitment) {
                accounts.push(Some((lookup.account_key, (*account).clone())));
            } else {
                db_indices.push(accounts.len());
                accounts.push(None);
            }
        }

        let db_results = reader.get_multi(db_indices.iter().map(|&i| &lookups[i].account_key));
        for (idx, result) in db_indices.into_iter().zip(db_results) {
            if let Some(account) = result? {
                accounts[idx] = Some((lookups[idx].account_key, account));
            }
        }

        let accounts: Vec<_> = accounts.into_iter().flatten().collect();

        Ok(Self {
            accounts: Rc::new(accounts),
            current_slot: slot,
            slot_hashes: Rc::new(slot_hashes),
        })
    }
}

impl Clone for SnapshotAddressLoader {
    fn clone(&self) -> Self {
        Self {
            accounts: Rc::clone(&self.accounts),
            current_slot: self.current_slot,
            slot_hashes: Rc::clone(&self.slot_hashes),
        }
    }
}

impl AddressLoader for SnapshotAddressLoader {
    fn load_addresses(
        self,
        lookups: &[solana_sdk::message::v0::MessageAddressTableLookup],
    ) -> Result<LoadedAddresses, AddressLoaderError> {
        let mut loaded_addresses = LoadedAddresses::default();
        for lookup in lookups {
            let account = self
                .accounts
                .iter()
                .find(|(pubkey, _)| pubkey == &lookup.account_key)
                .map(|(_, account)| account)
                .ok_or(AddressLoaderError::LookupTableAccountNotFound)?;

            if account.owner != address_lookup_table_program::id() {
                return Err(AddressLoaderError::InvalidAccountOwner);
            }

            let lookup_table = AddressLookupTable::deserialize(&account.data)
                .map_err(|_| AddressLoaderError::InvalidAccountData)?;

            let writable = lookup_table
                .lookup(
                    self.current_slot,
                    &lookup.writable_indexes,
                    &self.slot_hashes,
                )
                .map_err(map_address_lookup_error)?;
            loaded_addresses.writable.extend(writable);

            let readonly = lookup_table
                .lookup(
                    self.current_slot,
                    &lookup.readonly_indexes,
                    &self.slot_hashes,
                )
                .map_err(map_address_lookup_error)?;
            loaded_addresses.readonly.extend(readonly);
        }
        Ok(loaded_addresses)
    }
}

const fn map_address_lookup_error(error: AddressLookupError) -> AddressLoaderError {
    match error {
        AddressLookupError::LookupTableAccountNotFound => {
            AddressLoaderError::LookupTableAccountNotFound
        }
        AddressLookupError::InvalidAccountOwner => AddressLoaderError::InvalidAccountOwner,
        AddressLookupError::InvalidAccountData => AddressLoaderError::InvalidAccountData,
        AddressLookupError::InvalidLookupIndex => AddressLoaderError::InvalidLookupIndex,
    }
}

fn get_additional_mint_data(
    data: &[u8],
    unix_timestamp: UnixTimestamp,
) -> Result<SplTokenAdditionalDataV2, GetAccountsError> {
    StateWithExtensions::<Mint>::unpack(data)
        .map_err(|_| GetAccountsError::TokenMintUnpackFailed)
        .map(|mint| {
            let interest_bearing_config = mint
                .get_extension::<InterestBearingConfig>()
                .map(|x| (*x, unix_timestamp))
                .ok();
            let scaled_ui_amount_config = mint
                .get_extension::<ScaledUiAmountConfig>()
                .map(|x| (*x, unix_timestamp))
                .ok();
            SplTokenAdditionalDataV2 {
                decimals: mint.base.decimals,
                interest_bearing_config,
                scaled_ui_amount_config,
            }
        })
}
