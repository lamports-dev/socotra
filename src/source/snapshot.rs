use {
    crate::storage::rocksdb::Rocksdb,
    anyhow::Context,
    solana_accounts_db::accounts_file::{AccountsFile, StorageAccess},
    solana_sdk::{account::Account, clock::Slot},
    std::path::{Path, PathBuf},
    tokio::{fs, task::JoinSet},
    tokio_util::sync::CancellationToken,
    tracing::info,
};

const SNAPSHOT_READ_CONCURRENCY: usize = 64;

pub async fn read_snapshot_slot(snapshot_path: &Path) -> anyhow::Result<Slot> {
    let mut slot = 0;
    let mut dir = fs::read_dir(snapshot_path.join("snapshots")).await?;
    while let Some(entry) = dir.next_entry().await? {
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
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut account_files = vec![];
    let mut dir = fs::read_dir(snapshot_path.join("accounts")).await?;
    while let Some(entry) = dir.next_entry().await? {
        let size = entry.metadata().await?.len();
        account_files.push((entry.path(), size as usize));
    }
    info!(count = account_files.len(), "discovered account files");

    let mut workers = JoinSet::new();
    for (path, size) in account_files {
        if shutdown.is_cancelled() {
            break;
        }

        let db = db.clone();
        workers.spawn_blocking(move || {
            let accounts_file =
                AccountsFile::new_for_startup(&path, size, StorageAccess::default())
                    .with_context(|| format!("failed to open AccountsFile: {path:?}"))?;

            let mut batch = vec![];
            accounts_file
                .scan_accounts_without_data(|offset, _stored| {
                    accounts_file.get_stored_account_callback(offset, |full| {
                        batch.push((
                            *full.pubkey,
                            Account {
                                lamports: full.lamports,
                                data: full.data.to_vec(),
                                owner: *full.owner,
                                executable: full.executable,
                                rent_epoch: full.rent_epoch,
                            },
                        ));
                    });
                })
                .context("failed to scan accounts_file")?;
            db.store_accounts(&batch)?;

            anyhow::Ok(())
        });

        if workers.len() > SNAPSHOT_READ_CONCURRENCY
            && let Some(result) = workers.join_next().await
        {
            let _: () = result??;
        }
    }
    while let Some(result) = workers.join_next().await {
        let _: () = result??;
    }

    Ok(())
}
