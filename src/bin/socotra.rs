use {
    anyhow::Context,
    clap::Parser,
    socotra::{bank, config::Config, grpc, rocksdb::Rocksdb},
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
    let config = Config::load_from_file(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config))
        .unwrap();

    // Setup logs
    richat_shared::tracing::setup(config.logs.json)?;

    // Exit if we only check the config
    if args.check {
        info!("Config is OK!");
        return Ok(());
    }

    let shutdown = CancellationToken::new();

    let db = Rocksdb::open(config.storage)?;
    let confirmed_slot = match db.get_slot().context("failed to get confirmed slot")? {
        Some(slot) => slot,
        None => {
            let confirmed_slot =
                socotra::rpc::get_confirmed_slot(config.state_init.endpoint.clone())
                    .await
                    .context("failed to get confirmed slot")?;

            let sst_files = socotra::rpc::fetch_confirmed_state(
                confirmed_slot,
                &config.state_init,
                shutdown.clone(),
            )
            .await?;
            if shutdown.is_cancelled() {
                return Ok(());
            }

            db.consume_sst_files(sst_files)
                .context("failed to consume sst files")?;

            confirmed_slot
        }
    };

    let (update_tx, update_rx) = mpsc::channel(config.bank.channel_size);
    tokio::try_join!(
        grpc::subscribe(update_tx, config.source, confirmed_slot, shutdown.clone()),
        bank::start(db, update_rx, shutdown)
    )?;
    Ok(())
}
