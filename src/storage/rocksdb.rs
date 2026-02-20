use {
    crate::config::ConfigStorage,
    anyhow::Context,
    prost::encoding::encode_varint,
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, IngestExternalFileOptions,
        Options, WriteBatch,
    },
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
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
}

#[derive(Debug, Clone)]
pub struct Rocksdb {
    db: Arc<DB>,
    path: PathBuf,
    accounts_compression: DBCompressionType,
}

impl Rocksdb {
    pub fn open(config: ConfigStorage) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&config.path)
            .with_context(|| format!("failed to create db directory: {:?}", config.path))?;

        let accounts_compression = config.compression.into();

        let db_options = Self::get_db_options();
        let cf_descriptors = Self::cf_descriptors(accounts_compression);

        let db = Arc::new(
            DB::open_cf_descriptors(&db_options, &config.path, cf_descriptors)
                .with_context(|| format!("failed to open rocksdb with path: {:?}", config.path))?,
        );

        Ok(Self {
            db,
            path: config.path,
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

    #[instrument(name = "store_block", skip_all, fields(slot = state_slot_info.slot, accounts))]
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
}
