use {
    anyhow::Context,
    clap::Parser,
    futures::future::FutureExt,
    socotra::{bank, config::Config, grpc, rocksdb::Rocksdb},
    std::future::ready,
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
    tracing::info,
};

#[derive(Debug, Parser)]
#[clap(author, version)]
struct Args {
    /// Path to config
    #[clap(short, long, default_value_t = String::from("config.yml"))]
    pub config: String,

    /// Only check config and exit
    #[clap(long, default_value_t = false)]
    pub check: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config: Config = richat_shared::config::load_from_file(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config))?;

    // Setup logs
    richat_shared::tracing::setup(config.logs.json)?;

    // Exit if we only check the config
    if args.check {
        info!("Config is OK!");
        return Ok(());
    }

    let shutdown = CancellationToken::new();

    let db = Rocksdb::open(config.storage)?;
    let (latest_stored_slot, db_ready_fut) = match db
        .get_state_slot_info()
        .context("failed to get state slot")?
    {
        Some(value) => {
            info!(slot = value.slot, "latest stored slot (db)");
            (value, ready(Ok(db)).boxed())
        }
        None => {
            let value = socotra::rpc::get_confirmed_slot(config.state_init.endpoint.clone())
                .await
                .context("failed to get confirmed slot")?;
            info!(slot = value.slot, "latest stored slot (rpc)");

            let config = config.state_init.clone();
            let shutdown = shutdown.clone();
            let fetch_state_fut = async move {
                let sst_files =
                    socotra::rpc::fetch_confirmed_state(value.slot, &config, shutdown.clone())
                        .await?;
                if !shutdown.is_cancelled() {
                    db.consume_sst_files(sst_files, value)
                        .context("failed to consume sst files")?;
                }
                Ok::<_, anyhow::Error>(db)
            };

            (value, fetch_state_fut.boxed())
        }
    };

    let (update_tx, update_rx) = mpsc::channel(config.bank.updates_channel_size);
    tokio::try_join!(
        grpc::subscribe(
            update_tx,
            config.source,
            latest_stored_slot.slot + 1,
            shutdown.clone()
        ),
        bank::start(db_ready_fut, latest_stored_slot, update_rx, shutdown)
    )?;
    Ok(())
}
