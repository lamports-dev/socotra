use {
    crate::config::ConfigStorageRocksdbCompression,
    ahash::HashMap,
    anyhow::Context,
    bytes::Buf,
    prost::encoding::{decode_varint, encode_varint},
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, IngestExternalFileOptions,
        Options, WriteBatch,
    },
    solana_account_decoder::{
        parse_account_data::{AccountAdditionalDataV3, SplTokenAdditionalDataV2},
        parse_token::{get_token_account_mint, is_known_spl_token_id},
    },
    solana_sdk::{
        account::Account,
        clock::{Slot, UnixTimestamp},
        pubkey::Pubkey,
    },
    spl_token_2022_interface::{
        extension::{
            BaseStateWithExtensions, StateWithExtensions,
            interest_bearing_mint::InterestBearingConfig, scaled_ui_amount::ScaledUiAmountConfig,
        },
        state::Mint,
    },
    std::{
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

struct AccountIndexValue;

impl AccountIndexValue {
    fn encode(account: &Account, buf: &mut Vec<u8>) {
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

#[derive(Debug, thiserror::Error)]
pub enum GetAccountsError {
    #[error("rocksdb: {0}")]
    Rocksdb(#[from] rocksdb::Error),
    #[error("slot not found")]
    SlotNotFound,
    #[error("decode slot: {0}")]
    DecodeSlot(anyhow::Error),
    #[error("decode account: {0}")]
    DecodeAccount(#[from] prost::DecodeError),
    #[error("Invalid param: Token mint could not be unpacked")]
    TokenMintUnpackFailed,
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

    pub fn sst_config(&self, segment: u8) -> (PathBuf, Options) {
        let path = self.path.join(format!("{segment:03}.sst"));
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

        self.db
            .put_cf(self.cf_handle::<SlotIndexKey>(), "slot", slot_info.encode())
            .context("failed to store slot value")?;

        Ok(())
    }

    pub fn destroy(self) {
        if let Some(db) = Arc::into_inner(self.db) {
            drop(db);
            let _ = DB::destroy(&Options::default(), &self.path);
        }
    }

    pub fn get_state_slot_info(&self) -> anyhow::Result<Option<SlotIndexValue>> {
        self.db
            .get_cf(self.cf_handle::<SlotIndexKey>(), "slot")
            .context("failed to get slot data")?
            .map(|data| SlotIndexValue::decode(&data))
            .transpose()
    }

    #[instrument(skip_all, fields(slot = state_slot_info.slot, accounts))]
    pub fn store_new_state(
        &self,
        state_slot_info: SlotIndexValue,
        accounts: impl Iterator<Item = (Pubkey, Arc<Account>)>,
    ) -> anyhow::Result<()> {
        let span = info_span!("generate_batch").entered();
        let mut batch = WriteBatch::with_capacity_bytes(256 * 1024 * 1024); // 256MiB

        batch.put_cf(
            self.cf_handle::<SlotIndexKey>(),
            "slot",
            state_slot_info.encode(),
        );

        let mut num_accounts = 0u64;
        let mut buf = Vec::with_capacity(16 * 1024 * 1024); // 16MiB
        for (pubkey, account) in accounts {
            buf.clear();
            AccountIndexValue::encode(&account, &mut buf);
            batch.put_cf(
                self.cf_handle::<AccountIndexKey>(),
                AccountIndexKey::encode(&pubkey),
                &buf,
            );
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

    pub fn get_accounts(
        &self,
        pubkeys: &[Pubkey],
        accounts: &mut [Option<Arc<Account>>],
        json_parsed: bool,
        mints: &mut HashMap<Pubkey, AccountAdditionalDataV3>,
        get_account: impl Fn(&Pubkey) -> Option<Arc<Account>>,
    ) -> Result<(Slot, u64), GetAccountsError> {
        let snapshot = self.db.snapshot();
        let mut bytes_read = 0u64;

        let slot_data = snapshot
            .get_cf(self.cf_handle::<SlotIndexKey>(), "slot")?
            .ok_or(GetAccountsError::SlotNotFound)?;
        bytes_read += slot_data.len() as u64;
        let slot = SlotIndexValue::decode(&slot_data)
            .map_err(GetAccountsError::DecodeSlot)?
            .slot;

        let cf = self.cf_handle::<AccountIndexKey>();

        let indices: Vec<usize> = pubkeys
            .iter()
            .enumerate()
            .filter_map(|(i, pubkey)| {
                accounts[i] = get_account(pubkey);
                accounts[i].is_none().then_some(i)
            })
            .collect();

        let results = snapshot.multi_get_cf(
            indices
                .iter()
                .map(|&i| (cf, AccountIndexKey::encode(&pubkeys[i]))),
        );

        for (idx, result) in indices.into_iter().zip(results) {
            if let Some(data) = result? {
                bytes_read += data.len() as u64;
                accounts[idx] = Some(Arc::new(AccountIndexValue::decode(&data)?));
            }
        }

        if json_parsed {
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
                let clock_id = solana_sdk::sysvar::clock::id();
                let clock_account = get_account(&clock_id).or_else(|| {
                    let data = snapshot
                        .get_cf(cf, AccountIndexKey::encode(&clock_id))
                        .ok()
                        .flatten()?;
                    bytes_read += data.len() as u64;
                    AccountIndexValue::decode(&data).ok().map(Arc::new)
                });
                let unix_timestamp = clock_account
                    .and_then(|account| {
                        // Clock layout: slot(8) + epoch_start_timestamp(8) + epoch(8) + leader_schedule_epoch(8) + unix_timestamp(8)
                        account
                            .data
                            .get(32..40)
                            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
                    })
                    .unwrap_or(0);

                let mut mint_accounts: Vec<Option<Arc<Account>>> = vec![None; mint_pubkeys.len()];
                let db_mint_indices: Vec<usize> = mint_pubkeys
                    .iter()
                    .enumerate()
                    .filter_map(|(i, pubkey)| {
                        mint_accounts[i] = get_account(pubkey);
                        mint_accounts[i].is_none().then_some(i)
                    })
                    .collect();

                let mint_results = snapshot.multi_get_cf(
                    db_mint_indices
                        .iter()
                        .map(|&i| (cf, AccountIndexKey::encode(&mint_pubkeys[i]))),
                );

                for (idx, result) in db_mint_indices.into_iter().zip(mint_results) {
                    if let Some(data) = result? {
                        bytes_read += data.len() as u64;
                        mint_accounts[idx] = Some(Arc::new(AccountIndexValue::decode(&data)?));
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

        Ok((slot, bytes_read))
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
