use {
    anyhow::Context,
    prost::encoding::{encode_varint, encoded_len_varint},
    rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, Options, WriteBatch},
    solana_sdk::{clock::Epoch, pubkey::Pubkey},
    std::{fmt, path::Path, sync::Arc},
};

pub trait ColumnName {
    const NAME: &'static str;
}

#[derive(Debug)]
pub struct AccountIndex;

impl ColumnName for AccountIndex {
    const NAME: &'static str = "account_index";
}

impl AccountIndex {
    pub const fn key(pubkey: &Pubkey) -> [u8; 32] {
        pubkey.to_bytes()
    }

    pub fn decode(slice: &[u8]) -> anyhow::Result<Pubkey> {
        Pubkey::try_from(slice).context("failed to create pubkey")
    }
}

#[derive(Debug, Default, Clone)]
pub struct AccountIndexValue {
    pub lamports: u64,
    pub owner: Pubkey,
    pub data: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: Epoch,
}

impl AccountIndexValue {
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            encoded_len_varint(self.lamports)
                + 8
                + 32
                + encoded_len_varint(self.data.len() as u64)
                + self.data.len()
                + 1
                + encoded_len_varint(self.rent_epoch),
        );
        self.encode(&mut buf);
        buf
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint(self.lamports, buf);
        buf.extend_from_slice(&self.lamports.to_be_bytes());
        buf.extend_from_slice(self.owner.as_ref());
        encode_varint(self.data.len() as u64, buf);
        buf.extend_from_slice(&self.data);
        buf.push(if self.executable { 1 } else { 0 });
        encode_varint(self.rent_epoch, buf);
    }

    // fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {}
}

#[derive(Debug, Clone)]
pub struct Rocksdb {
    pub db: Arc<DB>,
}

impl Rocksdb {
    #[allow(clippy::type_complexity)]
    pub fn open<P>(path: P) -> anyhow::Result<Self>
    where
        P: AsRef<Path> + fmt::Debug,
    {
        let db_options = Self::get_db_options();
        let cf_descriptors = Self::cf_descriptors();

        let db = Arc::new(
            DB::open_cf_descriptors(&db_options, &path, cf_descriptors)
                .with_context(|| format!("failed to open rocksdb with path: {path:?}"))?,
        );

        Ok(Self { db })
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

    fn get_cf_options(compression: DBCompressionType) -> Options {
        let mut options = Options::default();

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

        // blobs
        // options.set_enable_blob_files(true);
        // options.set_min_blob_size(256);
        // options.set_blob_file_size(2 * 1024 * 1024 * 1024); // 2GiB
        // options.set_enable_blob_gc(true);
        // options.set_blob_gc_age_cutoff(0.5);
        // options.set_blob_gc_force_threshold(1.0);

        options
    }

    fn cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
        vec![Self::cf_descriptor::<AccountIndex>(DBCompressionType::None)]
    }

    fn cf_descriptor<C: ColumnName>(compression: DBCompressionType) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options(compression))
    }

    fn cf_handle<C: ColumnName>(db: &DB) -> &ColumnFamily {
        db.cf_handle(C::NAME)
            .expect("should never get an unknown column")
    }

    pub fn push_accounts(
        &self,
        accounts: impl Iterator<Item = (Pubkey, AccountIndexValue)>,
    ) -> anyhow::Result<()> {
        let mut batch = WriteBatch::with_capacity_bytes(128 * 1024 * 1024); // 128MiB

        let mut buf = vec![];
        for (pubkey, account) in accounts {
            buf.clear();
            account.encode(&mut buf);
            batch.put_cf(Self::cf_handle::<AccountIndex>(&self.db), pubkey, &buf);
        }

        self.db
            .write(batch)
            .context("failed to write accounts in batch")?;

        Ok(())
    }
}
