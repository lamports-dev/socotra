use {
    crate::config::ConfigStorage,
    anyhow::Context,
    prost::encoding::{encode_varint, encoded_len_varint},
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, IngestExternalFileOptions,
        Options, WriteBatch,
    },
    solana_sdk::{
        clock::{Epoch, Slot},
        pubkey::Pubkey,
    },
    std::{path::Path, sync::Arc, time::Instant},
    tracing::info,
};

pub trait ColumnName {
    const NAME: &'static str;
}

#[derive(Debug)]
pub struct SlotIndex;

impl ColumnName for SlotIndex {
    const NAME: &'static str = "slot_index";
}

#[derive(Debug)]
struct SlotIndexValue;

impl SlotIndexValue {
    const fn encode(slot: Slot) -> [u8; 8] {
        slot.to_be_bytes()
    }

    fn decode(slice: &[u8]) -> anyhow::Result<Slot> {
        let bytes: [u8; 8] = slice.try_into().context("invalid slot data length")?;
        Ok(Slot::from_be_bytes(bytes))
    }
}

#[derive(Debug)]
pub struct AccountIndex;

impl ColumnName for AccountIndex {
    const NAME: &'static str = "account_index";
}

impl AccountIndex {
    // pub const fn key(pubkey: &Pubkey) -> [u8; 32] {
    //     pubkey.to_bytes()
    // }

    // pub fn decode(slice: &[u8]) -> anyhow::Result<Pubkey> {
    //     Pubkey::try_from(slice).context("failed to create pubkey")
    // }
}

// #[derive(Debug, Default, Clone)]
// pub struct AccountIndexValue {
//     pub lamports: u64,
//     pub data: Vec<u8>,
//     pub owner: Pubkey,
//     pub executable: bool,
//     pub rent_epoch: Epoch,
// }

// impl AccountIndexValue {
//     pub fn encode_to_vec(&self) -> Vec<u8> {
//         let mut buf = Vec::with_capacity(
//             encoded_len_varint(self.lamports)
//                 + encoded_len_varint(self.data.len() as u64)
//                 + self.data.len()
//                 + 32
//                 + 1
//                 + encoded_len_varint(self.rent_epoch),
//         );
//         self.encode(&mut buf);
//         buf
//     }

//     pub fn encode(&self, buf: &mut Vec<u8>) {
//         encode_varint(self.lamports, buf);
//         encode_varint(self.data.len() as u64, buf);
//         buf.extend_from_slice(&self.data);
//         buf.extend_from_slice(self.owner.as_ref());
//         buf.push(if self.executable { 1 } else { 0 });
//         encode_varint(self.rent_epoch, buf);
//     }

//     // fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {}
// }

#[derive(Debug, Clone)]
pub struct Rocksdb {
    pub db: Arc<DB>,
}

impl Rocksdb {
    pub fn open(config: ConfigStorage) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&config.path)
            .with_context(|| format!("failed to create db directory: {:?}", config.path))?;

        let db_options = Self::get_db_options();
        let cf_descriptors = Self::cf_descriptors(config.compression.into());

        let db = Arc::new(
            DB::open_cf_descriptors(&db_options, &config.path, cf_descriptors)
                .with_context(|| format!("failed to open rocksdb with path: {:?}", config.path))?,
        );

        Ok(Self { db })
    }

    pub fn get_sst_options(compression: DBCompressionType) -> Options {
        let options = Self::get_db_options();
        Self::get_cf_options(Some(options), compression)
    }

    pub fn consume_sst_files<P>(&self, files: Vec<P>) -> anyhow::Result<()>
    where
        P: AsRef<Path>,
    {
        let ts = Instant::now();
        let mut ingest_options = IngestExternalFileOptions::default();
        ingest_options.set_move_files(true);
        ingest_options.set_snapshot_consistency(false);
        ingest_options.set_allow_global_seqno(false);
        ingest_options.set_allow_blocking_flush(false);
        self.db.ingest_external_file_cf_opts(
            self.db
                .cf_handle(AccountIndex::NAME)
                .expect("should never get an unknown column"),
            &ingest_options,
            files,
        )?;
        info!(elapsed = ?ts.elapsed(), "db created from sst files");

        Ok(())
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

    fn cf_descriptors(compression: DBCompressionType) -> Vec<ColumnFamilyDescriptor> {
        vec![Self::cf_descriptor::<AccountIndex>(compression)]
    }

    fn cf_descriptor<C: ColumnName>(compression: DBCompressionType) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options(None, compression))
    }

    fn cf_handle<C: ColumnName>(db: &DB) -> &ColumnFamily {
        db.cf_handle(C::NAME)
            .expect("should never get an unknown column")
    }

    pub fn get_slot(&self) -> anyhow::Result<Option<Slot>> {
        self.db
            .get_cf(Self::cf_handle::<SlotIndex>(&self.db), "slot")?
            .map(|data| SlotIndexValue::decode(&data))
            .transpose()
    }

    // pub fn push_accounts(
    //     &self,
    //     accounts: impl Iterator<Item = (Pubkey, AccountIndexValue)>,
    // ) -> anyhow::Result<()> {
    //     let mut batch = WriteBatch::with_capacity_bytes(256 * 1024 * 1024); // 256MiB

    //     let mut buf = vec![];
    //     for (pubkey, account) in accounts {
    //         buf.clear();
    //         account.encode(&mut buf);
    //         batch.put_cf(Self::cf_handle::<AccountIndex>(&self.db), pubkey, &buf);
    //     }

    //     self.db
    //         .write(batch)
    //         .context("failed to write accounts in batch")?;

    //     Ok(())
    // }
}
