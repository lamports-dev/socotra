use {
    anyhow::Context,
    indicatif::{ProgressBar, ProgressStyle},
    socotra::rocksdb::{AccountIndex, AccountIndexValue, ColumnName, Rocksdb},
    solana_accounts_db::accounts_file::{AccountsFile, StorageAccess},
    std::{
        collections::BTreeMap,
        io::{self, IsTerminal},
        path::Path,
        time::Instant,
    },
    tokio::{fs, sync::mpsc, task::JoinSet},
    tracing::info,
    tracing_subscriber::{
        filter::{EnvFilter, LevelFilter},
        fmt::layer,
        layer::SubscriberExt,
        util::SubscriberInitExt,
    },
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?;

    tracing_subscriber::registry()
        .with(env)
        .with(
            layer()
                .with_ansi(io::stdout().is_terminal() && io::stderr().is_terminal())
                .with_line_number(true),
        )
        .try_init()?;

    let location_snapshot = "/home/ubuntu/storage/snapshots/snapshot-370229901";
    // let location_db = "/home/ubuntu/storage/accounts-db";

    info!(count = num_cpus::get(), "available cpus");
    // let db = Rocksdb::open(location_db)?;

    let mut slot = 0;
    let mut dir = fs::read_dir(Path::new(location_snapshot).join("snapshots")).await?;
    while let Some(entry) = dir.next_entry().await? {
        if let Some(Ok(snapshot_slot)) = entry.file_name().to_str().map(|s| s.parse::<u64>()) {
            slot = slot.max(snapshot_slot);
        }
    }
    info!(slot, "read snapshot slot");

    let ts = Instant::now();
    let mut accounts = vec![];
    let mut dir = fs::read_dir(Path::new(location_snapshot).join("accounts")).await?;
    while let Some(entry) = dir.next_entry().await? {
        let size = entry.metadata().await?.len();
        accounts.push((entry.path(), size as usize));
    }
    info!(elapsed = ?ts.elapsed(), accounts = accounts.len(), "read accounts file names");

    // read accounts
    let (tx, mut rx) = mpsc::channel(16 * 1024);
    let jh = tokio::spawn(async move {
        let mut workers = JoinSet::new();
        for (path, size) in accounts {
            let tx = tx.clone();
            workers.spawn_blocking(move || {
                let (accounts, _usize) =
                    AccountsFile::new_from_file(&path, size, StorageAccess::default())
                        .with_context(|| format!("failed to open AccountsFile: {path:?}"))?;

                let mut values = vec![];
                accounts.scan_accounts(|_offset, account| {
                    values.push((
                        *account.pubkey,
                        AccountIndexValue {
                            lamports: account.lamports,
                            owner: *account.owner,
                            data: account.data.to_vec(),
                            executable: account.executable,
                            rent_epoch: account.rent_epoch,
                        }
                        .encode_to_vec(),
                    ));
                });
                tx.blocking_send(values)
                    .expect("failed to send account to the channel");

                Ok::<(), anyhow::Error>(())
            });

            if workers.len() > 256
                && let Some(result) = workers.join_next().await
            {
                let _: () = result??;
            }
        }
        while let Some(result) = workers.join_next().await {
            let _: () = result??;
        }

        Ok::<(), anyhow::Error>(())
    });

    let pb = ProgressBar::no_length();
    pb.set_style(ProgressStyle::with_template(
        "{elapsed_precise} total accounts: {pos}",
    )?);
    let mut map = BTreeMap::new();
    while let Some(values) = rx.recv().await {
        pb.inc(values.len() as u64);
        for (pubkey, account) in values {
            map.insert(pubkey, account);
        }
    }
    // let mut workers = JoinSet::new();
    // let mut size = 0;
    // let mut accounts = vec![];
    // while let Some(values) = rx.recv().await {
    //     pb.inc(values.len() as u64);

    //     for (pubkey, account) in values {
    //         size += account.data.len();
    //         accounts.push((pubkey, account));
    //     }

    //     if size >= 256 * 1024 * 1024 {
    //         size = 0;
    //         let db = db.clone();
    //         let accounts = std::mem::take(&mut accounts);
    //         workers.spawn_blocking(move || db.push_accounts(accounts.into_iter()));
    //     }

    //     if workers.len() > 64
    //         && let Some(result) = workers.join_next().await
    //     {
    //         let _: () = result??;
    //     }
    // }
    // workers.spawn_blocking(move || db.push_accounts(accounts.into_iter()));
    // while let Some(result) = workers.join_next().await {
    //     let _: () = result??;
    // }
    pb.finish();

    let _: () = jh.await??;

    drop(rx);
    use {
        itertools::Itertools,
        rocksdb::{
            ColumnFamilyDescriptor, DB, DBCompressionType, IngestExternalFileOptions, Options,
            SstFileWriter,
        },
        std::sync::Arc,
        tokio::sync::Semaphore,
    };
    fn create_options() -> Options {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_max_background_jobs(num_cpus::get() as i32);
        options.set_max_total_wal_size(4 * 1024 * 1024 * 1024);
        const MAX_WRITE_BUFFER_SIZE: u64 = 512 * 1024 * 1024;
        options.set_max_write_buffer_number(8);
        options.set_write_buffer_size(MAX_WRITE_BUFFER_SIZE as usize);
        let file_num_compaction_trigger = 4;
        let total_size_base = MAX_WRITE_BUFFER_SIZE * file_num_compaction_trigger;
        let file_size_base = total_size_base / 10;
        options.set_level_zero_file_num_compaction_trigger(file_num_compaction_trigger as i32);
        options.set_max_bytes_for_level_base(total_size_base);
        options.set_target_file_size_base(file_size_base);
        options.set_compression_type(DBCompressionType::None);
        options.set_writable_file_max_buffer_size(8 * 1024 * 1024);
        options
    }
    let ts = Instant::now();
    let mut workers = vec![];
    let sem = Arc::new(Semaphore::new(64));
    for chunk in &map.into_iter().chunks(1_000_000) {
        let accounts = chunk.collect::<Vec<_>>();
        let id = workers.len();
        let sem = Arc::clone(&sem);
        workers.push(tokio::spawn(async move {
            let _lock = sem.acquire().await;
            tokio::task::spawn_blocking(move || {
                let path = format!("/home/ubuntu/storage/accounts-db/sst/{id:010}.sst");
                let options = create_options();
                let mut sst = SstFileWriter::create(&options);
                sst.open(&path)?;
                for (pubkey, account) in accounts {
                    sst.put(pubkey, account)?;
                }
                sst.finish()?;
                Ok::<_, anyhow::Error>(path)
            })
            .await?
        }));
    }
    let files = futures::future::try_join_all(workers)
        .await?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    info!(elapsed = ?ts.elapsed(), "build sst files");

    let ts = Instant::now();
    let db = DB::open_cf_descriptors(
        &create_options(),
        "/home/ubuntu/storage/accounts-db/db",
        vec![ColumnFamilyDescriptor::new(
            AccountIndex::NAME,
            create_options(),
        )],
    )?;
    let mut ingest_options = IngestExternalFileOptions::default();
    ingest_options.set_move_files(true);
    ingest_options.set_snapshot_consistency(false);
    ingest_options.set_allow_global_seqno(false);
    ingest_options.set_allow_blocking_flush(false);
    db.ingest_external_file_cf_opts(
        db.cf_handle(AccountIndex::NAME)
            .expect("should never get an unknown column"),
        &ingest_options,
        files,
    )?;
    info!(elapsed = ?ts.elapsed(), "created db");

    Ok(())
}
