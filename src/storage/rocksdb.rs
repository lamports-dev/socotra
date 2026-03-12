use {
    crate::{
        config::ConfigStorageRocksdbCompression,
        metrics::{READ_ACCOUNTS_BYTES_TOTAL, READ_ACCOUNTS_SECONDS_TOTAL, READ_ACCOUNTS_TOTAL},
        storage::reader::ReaderState,
    },
    agave_feature_set::FeatureSet,
    ahash::HashMap,
    anyhow::Context,
    bytes::Buf,
    litesvm::{LiteSVM, error::LiteSVMError},
    metrics::{counter, gauge},
    prost::encoding::{decode_varint, encode_varint},
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, IngestExternalFileOptions,
        Options, WriteBatch,
    },
    serde::de::DeserializeOwned,
    solana_account_decoder::{
        UiAccountEncoding,
        parse_account_data::{AccountAdditionalDataV3, SplTokenAdditionalDataV2},
        parse_token::{get_token_account_mint, is_known_spl_token_id},
    },
    solana_address_lookup_table_interface::{
        error::AddressLookupError, program as address_lookup_table_program,
        state::AddressLookupTable,
    },
    solana_commitment_config::CommitmentLevel,
    solana_loader_v3_interface::state::UpgradeableLoaderState,
    solana_message::inner_instruction::InnerInstructionsList,
    solana_rpc_client_types::{
        config::RpcSimulateTransactionAccountsConfig, response::RpcBlockhash,
    },
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_sdk::{
        account::Account,
        clock::{Clock, MAX_PROCESSING_AGE, Slot, UnixTimestamp},
        hash::Hash,
        message::{AddressLoader, v0::LoadedAddresses},
        pubkey::Pubkey,
        slot_hashes::SlotHashes,
        sysvar::SysvarId,
    },
    solana_sdk_ids::{bpf_loader_upgradeable, sysvar},
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction::{sanitized::MessageHash, versioned::VersionedTransaction},
    solana_transaction_context::TransactionReturnData,
    solana_transaction_error::{AddressLoaderError, TransactionResult},
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

pub struct GetSimulateTransactionData {
    pub slot: Slot,
    pub result: TransactionResult<()>,
    pub logs: Vec<String>,
    pub units_consumed: u64,
    pub return_data: TransactionReturnData,
    pub inner_instructions: Option<InnerInstructionsList>,
    pub fee: u64,
    pub replacement_blockhash: Option<RpcBlockhash>,
    /// (pubkey, Option<Account>), encoding from config_accounts.
    /// `None` means no accounts were requested.
    #[allow(clippy::type_complexity)]
    pub post_simulation_accounts: Option<(Vec<(Pubkey, Option<Account>)>, UiAccountEncoding)>,
    pub loaded_accounts_data_size: u32,
    pub loaded_addresses: LoadedAddresses,
}

impl std::fmt::Debug for GetSimulateTransactionData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetSimulateTransactionData")
            .field("slot", &self.slot)
            .field("result", &self.result)
            .finish_non_exhaustive()
    }
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
    feature_set: FeatureSet,
}

impl Rocksdb {
    pub fn open(
        path: PathBuf,
        compression: ConfigStorageRocksdbCompression,
        feature_set: FeatureSet,
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
            feature_set,
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

    #[allow(clippy::too_many_arguments)]
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
                let clock: Clock = reader.get_sysvar(state, commitment)?;
                let unix_timestamp = clock.unix_timestamp;

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
            unsanitized_tx.clone(),
            MessageHash::Compute,
            None,
            &address_loader,
            &Default::default(), // reserved_account_keys
            self.feature_set
                .is_active(&agave_feature_set::static_instruction_limit::id()),
        )
        .map_err(|err| {
            GetSimulateTransactionDataError::InvalidParams(format!("invalid transaction: {err}"))
        })?;

        let verification_error = if sig_verify {
            transaction.verify().err()
        } else {
            None
        };

        if let Some(err) = verification_error {
            return Ok(GetSimulateTransactionData {
                slot,
                result: Err(err),
                logs: Vec::new(),
                units_consumed: 0,
                return_data: TransactionReturnData::default(),
                inner_instructions: None,
                fee: 0,
                replacement_blockhash,
                post_simulation_accounts: None,
                loaded_accounts_data_size: 0,
                loaded_addresses: LoadedAddresses::default(),
            });
        }

        // Create LiteSVM and load state
        let mut svm = LiteSVM::default()
            .with_feature_set(self.feature_set.clone())
            .with_builtins()
            .with_sigverify(false)
            .with_blockhash_check(false)
            .with_transaction_history(0);

        reader.svm_load_sysvars(&mut svm, state, commitment)?;

        let loaded_addresses = (&address_loader)
            .load_addresses(
                unsanitized_tx
                    .message
                    .address_table_lookups()
                    .unwrap_or_default(),
            )
            .map_err(|e| {
                GetSimulateTransactionDataError::InvalidParams(format!("address lookup: {e}"))
            })?;

        // Load ALT
        let num_lookup_tables = address_loader.accounts.len();
        let mut loaded_accounts_data_size = (num_lookup_tables * 8248) as u32;
        for (pubkey, account) in address_loader.accounts {
            svm.set_account(pubkey, account)
                .map_err(AccountReaderError::from)?;
        }

        // Load all accounts referenced by the transaction
        let account_keys: Vec<Pubkey> = transaction.account_keys().iter().copied().collect();
        loaded_accounts_data_size +=
            reader.svm_load_accounts(&mut svm, &account_keys, state, commitment)?;

        let (result, logs, units_consumed, return_data, inner_instructions, fee, post_accounts) =
            match svm.simulate_transaction(unsanitized_tx) {
                Ok(info) => (
                    Ok(()),
                    info.meta.logs,
                    info.meta.compute_units_consumed,
                    info.meta.return_data,
                    info.meta.inner_instructions,
                    info.meta.fee,
                    info.post_accounts,
                ),
                Err(failed) => (
                    Err(failed.err),
                    failed.meta.logs,
                    failed.meta.compute_units_consumed,
                    failed.meta.return_data,
                    failed.meta.inner_instructions,
                    failed.meta.fee,
                    Vec::new(),
                ),
            };

        let post_simulation_accounts = config_accounts.map(|config| {
            let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64);
            let accounts = config
                .addresses
                .iter()
                .map(|s| {
                    let pk: Pubkey = s.parse().unwrap_or_default();
                    let acct = post_accounts
                        .iter()
                        .find(|(k, _)| *k == pk)
                        .map(|(_, a)| Account::from(a.clone()));
                    (pk, acct)
                })
                .collect();
            (accounts, encoding)
        });

        Ok(GetSimulateTransactionData {
            slot,
            result,
            logs,
            units_consumed,
            return_data,
            inner_instructions: if enable_cpi_recording {
                Some(inner_instructions)
            } else {
                None
            },
            fee,
            replacement_blockhash,
            post_simulation_accounts,
            loaded_accounts_data_size,
            loaded_addresses,
        })
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
    #[error("sysvar not found: {0}")]
    SysvarNotFound(Pubkey),
    #[error("bincode deserialize: {0}")]
    BincodeDeserialize(#[from] bincode::Error),
    #[error("svm set account: {0}")]
    SvmSetAccount(#[from] LiteSVMError),
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

    /// Load sysvar account, only valid for accounts that should be updated every slot.
    fn get_sysvar<T: DeserializeOwned + SysvarId>(
        &mut self,
        state: &ReaderState,
        commitment: CommitmentLevel,
    ) -> Result<T, AccountReaderError> {
        let pubkey = &T::id();
        let account = match commitment {
            // Check processed_map first, fallback to confirmed_map only when heights match.
            CommitmentLevel::Processed => {
                let account = state.processed_map.get(pubkey).or_else(|| {
                    (state.processed_height == state.confirmed_height)
                        .then(|| state.confirmed_map.get(pubkey))
                        .flatten()
                });
                account
                    .map(|a| (**a).clone())
                    .ok_or(AccountReaderError::SysvarNotFound(*pubkey))
            }
            CommitmentLevel::Confirmed => state
                .confirmed_map
                .get(pubkey)
                .map(|a| (**a).clone())
                .ok_or(AccountReaderError::SysvarNotFound(*pubkey)),
            CommitmentLevel::Finalized => self
                .get_multi(std::iter::once(pubkey))
                .next()
                .unwrap_or(Ok(None))?
                .ok_or(AccountReaderError::SysvarNotFound(*pubkey)),
        }?;
        bincode::deserialize(&account.data).map_err(AccountReaderError::BincodeDeserialize)
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

    fn svm_load_sysvars(
        &mut self,
        svm: &mut LiteSVM,
        state: &ReaderState,
        commitment: CommitmentLevel,
    ) -> Result<(), AccountReaderError> {
        // Slot-changing sysvars need get_sysvar-style commitment logic.
        let slot_changing = [
            sysvar::clock::id(),
            sysvar::slot_hashes::id(),
            sysvar::slot_history::id(),
            sysvar::recent_blockhashes::id(),
        ];
        // Other sysvars use normal get_account logic.
        let other = [
            sysvar::rent::id(),
            sysvar::epoch_schedule::id(),
            sysvar::stake_history::id(),
            sysvar::fees::id(),
            sysvar::epoch_rewards::id(),
            sysvar::last_restart_slot::id(),
        ];

        let mut resolved: Vec<(Pubkey, Account)> =
            Vec::with_capacity(slot_changing.len() + other.len());
        let mut db_needed: Vec<Pubkey> = Vec::new();

        // Resolve slot-changing sysvars from maps using strict commitment logic.
        for pubkey in &slot_changing {
            match commitment {
                CommitmentLevel::Processed => {
                    let account = state.processed_map.get(pubkey).or_else(|| {
                        (state.processed_height == state.confirmed_height)
                            .then(|| state.confirmed_map.get(pubkey))
                            .flatten()
                    });
                    let account = account
                        .map(|a| (**a).clone())
                        .ok_or(AccountReaderError::SysvarNotFound(*pubkey))?;
                    resolved.push((*pubkey, account));
                }
                CommitmentLevel::Confirmed => {
                    let account = state
                        .confirmed_map
                        .get(pubkey)
                        .map(|a| (**a).clone())
                        .ok_or(AccountReaderError::SysvarNotFound(*pubkey))?;
                    resolved.push((*pubkey, account));
                }
                CommitmentLevel::Finalized => {
                    db_needed.push(*pubkey);
                }
            }
        }

        // Resolve other sysvars from maps, fall back to db.
        for pubkey in &other {
            if let Some(acct) = state.get_account(pubkey, commitment) {
                resolved.push((*pubkey, (*acct).clone()));
            } else {
                db_needed.push(*pubkey);
            }
        }

        // Single batched db read for all unresolved sysvars.
        if !db_needed.is_empty() {
            for (result, pubkey) in self.get_multi(db_needed.iter()).zip(db_needed.into_iter()) {
                let account = result?.ok_or(AccountReaderError::SysvarNotFound(pubkey))?;
                resolved.push((pubkey, account));
            }
        }

        for (pubkey, account) in resolved {
            svm.set_account(pubkey, account)?;
        }

        Ok(())
    }

    fn svm_load_accounts(
        &mut self,
        svm: &mut LiteSVM,
        pubkeys: &[Pubkey],
        state: &ReaderState,
        commitment: CommitmentLevel,
    ) -> Result<u32, AccountReaderError> {
        // Partition keys into cached vs db-needed
        let mut resolved: Vec<(Pubkey, Account)> = Vec::new();
        let mut db_needed: Vec<Pubkey> = Vec::new();
        for pubkey in pubkeys {
            if let Some(acct) = state.get_account(pubkey, commitment) {
                resolved.push((*pubkey, (*acct).clone()));
            } else {
                db_needed.push(*pubkey);
            }
        }

        // Batch load from DB
        for (pubkey, result) in db_needed.iter().zip(self.get_multi(db_needed.iter())) {
            if let Some(account) = result? {
                resolved.push((*pubkey, account));
            }
        }

        // Identify BPF upgradeable programdata addresses
        let mut programdata_keys: Vec<Pubkey> = Vec::new();
        for (_, account) in &resolved {
            if account.executable && account.owner == bpf_loader_upgradeable::id() {
                let UpgradeableLoaderState::Program {
                    programdata_address,
                } = bincode::deserialize::<UpgradeableLoaderState>(&account.data)?
                else {
                    continue;
                };
                programdata_keys.push(programdata_address);
            }
        }

        // Batch load programdata: partition into cached vs db-needed
        let mut pd_db_needed: Vec<Pubkey> = Vec::new();
        for pubkey in &programdata_keys {
            if let Some(acct) = state.get_account(pubkey, commitment) {
                resolved.push((*pubkey, (*acct).clone()));
            } else {
                pd_db_needed.push(*pubkey);
            }
        }
        for (result, pubkey) in self
            .get_multi(pd_db_needed.iter())
            .zip(pd_db_needed.into_iter())
        {
            if let Some(account) = result? {
                resolved.push((pubkey, account));
            }
        }

        // SIMD-0186: base size per account
        const ACCOUNT_BASE_SIZE: usize = 64;

        let loaded_accounts_data_size: u32 = resolved
            .iter()
            .map(|(_, account)| (ACCOUNT_BASE_SIZE + account.data.len()) as u32)
            .sum();

        // Set all accounts on SVM
        for (pubkey, account) in resolved {
            svm.set_account(pubkey, account)?;
        }

        Ok(loaded_accounts_data_size)
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
    accounts: Vec<(Pubkey, Account)>,
    current_slot: Slot,
    slot_hashes: SlotHashes,
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
                accounts: Vec::new(),
                current_slot: slot,
                slot_hashes: SlotHashes::default(),
            });
        }

        // Load SlotHashes sysvar
        let slot_hashes: SlotHashes = reader.get_sysvar(state, commitment)?;

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
            accounts,
            current_slot: slot,
            slot_hashes,
        })
    }
}

impl AddressLoader for &SnapshotAddressLoader {
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
