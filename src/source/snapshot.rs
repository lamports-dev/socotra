use {
    crate::storage::rocksdb::{AccountIndexKey, AccountIndexValue, Rocksdb},
    anyhow::Context,
    rocksdb::SstFileWriter,
    solana_accounts_db::accounts_file::{AccountsFile, StorageAccess},
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::{
        collections::BTreeMap,
        fs::{self, File},
        io::{BufWriter, Read, Seek, SeekFrom, Write},
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Instant,
    },
    tokio::{
        sync::{Semaphore, mpsc},
        task::JoinSet,
    },
    tokio_util::sync::CancellationToken,
    tracing::info,
};

const NUM_BUCKETS: usize = 256;

type AccountBatch = Vec<(Pubkey, Vec<u8>)>;
type BucketIndex = BTreeMap<Pubkey, (u64, u32)>;

fn shard_path(db_path: &Path, shard_id: usize) -> PathBuf {
    db_path.join(format!("snapshot_shard_{shard_id}.bin"))
}

pub fn read_snapshot_slot(snapshot_path: &Path) -> anyhow::Result<Slot> {
    let mut slot = 0;
    for entry in fs::read_dir(snapshot_path.join("snapshots"))? {
        let entry = entry?;
        if let Some(Ok(snapshot_slot)) = entry.file_name().to_str().map(|s| s.parse::<Slot>()) {
            slot = slot.max(snapshot_slot);
        }
    }
    info!(slot, "read snapshot slot");
    Ok(slot)
}

pub async fn load_snapshot_accounts(
    db: Rocksdb,
    snapshot_path: PathBuf,
    db_path: PathBuf,
    accounts_read_concurrency: usize,
    sst_write_concurrency: usize,
    num_shards: usize,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        num_shards > 0 && num_shards <= NUM_BUCKETS && num_shards.is_power_of_two(),
        "num_shards must be a power of 2, > 0 and <= {NUM_BUCKETS}, got {num_shards}"
    );
    let buckets_per_shard = NUM_BUCKETS / num_shards;
    // Discover account files
    let mut account_files = vec![];
    for entry in fs::read_dir(snapshot_path.join("accounts"))? {
        let entry = entry?;
        let size = entry.metadata()?.len();
        account_files.push((entry.path(), size as usize));
    }
    info!(count = account_files.len(), "discovered account files");

    // Phase 1: Parse account files → 16 shard flat files + 256 bucket indexes
    let ts = Instant::now();
    // Create 16 mpsc channels (one per shard)
    let mut shard_txs = Vec::with_capacity(num_shards);
    let mut writer_handles = Vec::with_capacity(num_shards);

    for shard_id in 0..num_shards {
        let (tx, rx) = mpsc::channel::<AccountBatch>(accounts_read_concurrency);
        shard_txs.push(tx);

        let shard_path = shard_path(&db_path, shard_id);
        let handle = tokio::task::spawn_blocking(move || {
            let mut flat_file = BufWriter::with_capacity(
                64 * 1024 * 1024,
                File::create(&shard_path)
                    .with_context(|| format!("failed to create shard file: {shard_path:?}"))?,
            );
            let mut buckets: Vec<BucketIndex> =
                (0..buckets_per_shard).map(|_| BTreeMap::new()).collect();
            let mut offset: u64 = 0;
            let mut rx = rx;

            while let Some(batch) = rx.blocking_recv() {
                for (pubkey, encoded) in batch {
                    flat_file
                        .write_all(&encoded)
                        .context("failed to write to shard file")?;
                    let size = encoded.len() as u32;
                    let bucket = pubkey.as_ref()[0] as usize;
                    let bucket_local = bucket - shard_id * buckets_per_shard;
                    buckets[bucket_local].insert(pubkey, (offset, size));
                    offset += size as u64;
                }
            }
            flat_file.flush().context("failed to flush shard file")?;
            anyhow::Ok(buckets)
        });
        writer_handles.push(handle);
    }

    // Reader workers parse account files and produce batches
    let mut workers = JoinSet::new();
    for (path, size) in account_files {
        if shutdown.is_cancelled() {
            break;
        }

        workers.spawn_blocking(move || {
            let accounts_file =
                AccountsFile::new_for_startup(&path, size, StorageAccess::default())
                    .with_context(|| format!("failed to open AccountsFile: {path:?}"))?;

            let mut batch = vec![];
            let mut buf = Vec::with_capacity(4096);
            accounts_file
                .scan_accounts_without_data(|offset, _stored| {
                    accounts_file.get_stored_account_callback(offset, |full| {
                        buf.clear();
                        AccountIndexValue::encode(
                            &Account {
                                lamports: full.lamports,
                                data: full.data.to_vec(),
                                owner: *full.owner,
                                executable: full.executable,
                                rent_epoch: full.rent_epoch,
                            },
                            &mut buf,
                        );
                        batch.push((*full.pubkey, buf.clone()));
                    });
                })
                .context("failed to scan accounts_file")?;

            anyhow::Ok(batch)
        });

        if workers.len() > accounts_read_concurrency
            && let Some(result) = workers.join_next().await
        {
            dispatch_batch(result??, &shard_txs, num_shards, buckets_per_shard).await?;
        }
    }
    while let Some(result) = workers.join_next().await {
        dispatch_batch(result??, &shard_txs, num_shards, buckets_per_shard).await?;
    }
    drop(shard_txs);

    // Collect results from all 16 writer tasks → 256 BTreeMaps total
    let mut all_buckets: Vec<(usize, BucketIndex)> = Vec::with_capacity(256);
    for (shard_id, handle) in writer_handles.into_iter().enumerate() {
        let buckets = handle.await.context("shard writer panicked")??;
        for (local_idx, bucket_map) in buckets.into_iter().enumerate() {
            let global_bucket = shard_id * buckets_per_shard + local_idx;
            all_buckets.push((global_bucket, bucket_map));
        }
    }

    let total_accounts: usize = all_buckets.iter().map(|(_, m)| m.len()).sum();
    info!(
        accounts = total_accounts,
        elapsed = ?ts.elapsed(),
        "parsed accounts into shard files"
    );

    if shutdown.is_cancelled() {
        for shard_id in 0..num_shards {
            let _ = fs::remove_file(shard_path(&db_path, shard_id));
        }
        return Ok(());
    }

    // Phase 2: Create 256 SST files from sorted bucket indexes + shard flat files
    let ts = Instant::now();
    let num_sst = all_buckets.iter().filter(|(_, m)| !m.is_empty()).count();

    let semaphore = Arc::new(Semaphore::new(sst_write_concurrency));
    let mut sst_workers = JoinSet::new();
    let mut sst_files = Vec::with_capacity(num_sst);

    let mut shard_remaining = vec![0usize; num_shards];
    for (bucket_id, bucket_map) in &all_buckets {
        if !bucket_map.is_empty() {
            shard_remaining[bucket_id / buckets_per_shard] += 1;
        }
    }
    let shard_remaining: Vec<Arc<AtomicUsize>> = shard_remaining
        .into_iter()
        .map(|n| Arc::new(AtomicUsize::new(n)))
        .collect();
    // Delete shards that have zero non-empty buckets immediately
    for (shard_id, counter) in shard_remaining.iter().enumerate() {
        if counter.load(Ordering::Relaxed) == 0 {
            let _ = fs::remove_file(shard_path(&db_path, shard_id));
        }
    }

    for (bucket_id, bucket_map) in all_buckets {
        if bucket_map.is_empty() {
            continue;
        }

        let shard_id = bucket_id / buckets_per_shard;
        let shard_path = shard_path(&db_path, shard_id);
        let (sst_path, options) = db.sst_config(bucket_id as u16);
        sst_files.push(sst_path.clone());
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .context("semaphore closed")?;

        let remaining = Arc::clone(&shard_remaining[shard_id]);
        sst_workers.spawn_blocking(move || {
            let _permit = permit;
            let mut reader =
                File::open(&shard_path).context("failed to open shard file for reading")?;
            let mut sst = SstFileWriter::create(&options);
            sst.open(&sst_path)
                .with_context(|| format!("failed to open SST file: {sst_path:?}"))?;

            let mut read_buf = Vec::new();
            for (pubkey, (offset, size)) in bucket_map {
                read_buf.resize(size as usize, 0);
                reader
                    .seek(SeekFrom::Start(offset))
                    .context("failed to seek in shard file")?;
                reader
                    .read_exact(&mut read_buf)
                    .context("failed to read from shard file")?;
                sst.put(AccountIndexKey::encode(&pubkey), &read_buf)
                    .context("failed to put key into SST")?;
            }

            sst.finish().context("failed to finish SST file")?;
            if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                let _ = fs::remove_file(&shard_path);
            }
            anyhow::Ok(())
        });
    }

    while let Some(result) = sst_workers.join_next().await {
        let _: () = result??;
    }
    info!(num_sst, elapsed = ?ts.elapsed(), "SST files created");

    // Phase 3: Ingest
    db.sst_ingest_files(sst_files)?;

    Ok(())
}

/// Partition a batch by shard and send each sub-batch to the right channel.
async fn dispatch_batch(
    batch: AccountBatch,
    shard_txs: &[mpsc::Sender<AccountBatch>],
    num_shards: usize,
    buckets_per_shard: usize,
) -> anyhow::Result<()> {
    let mut per_shard: Vec<AccountBatch> = (0..num_shards).map(|_| Vec::new()).collect();

    for (pubkey, encoded) in batch {
        let shard = pubkey.as_ref()[0] as usize / buckets_per_shard;
        per_shard[shard].push((pubkey, encoded));
    }

    for (shard_id, sub_batch) in per_shard.into_iter().enumerate() {
        if !sub_batch.is_empty() {
            shard_txs[shard_id]
                .send(sub_batch)
                .await
                .map_err(|_| anyhow::anyhow!("shard writer {shard_id} dropped"))?;
        }
    }

    Ok(())
}
