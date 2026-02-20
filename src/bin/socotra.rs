use {
    anyhow::Context,
    clap::Parser,
    futures::future::FutureExt,
    signal_hook::{
        consts::{SIGINT, SIGTERM},
        iterator::Signals,
    },
    socotra::{
        config::Config,
        source,
        storage::{blocks, rocksdb::Rocksdb},
    },
    std::{
        future::ready,
        thread::{self, sleep},
        time::Duration,
    },
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
    tracing::{info, warn},
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

fn main() {
    if let Err(err) = try_main() {
        match std::env::var_os("RUST_BACKTRACE") {
            Some(value) if value == *"0" => eprintln!("Error: {err}"),
            None => eprintln!("Error: {err}"),
            _ => eprintln!("Error: {err:?}"),
        }
        std::process::exit(1);
    }
}

fn try_main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config: Config = richat_shared::config::load_from_file_sync(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config))?;

    // Setup logs
    richat_shared::tracing::setup(config.logs.json)?;

    // Exit if we only check the config
    if args.check {
        info!("Config is OK!");
        return Ok(());
    }

    // Open storage
    let db = Rocksdb::open(config.storage)?;

    // Shutdown
    let mut threads = Vec::<(String, _)>::with_capacity(4);
    let shutdown = CancellationToken::new();

    let jh = thread::Builder::new().name("socSource".to_owned()).spawn({
        let shutdown = shutdown.clone();
        let runtime = config.source.tokio.clone().build_runtime("socSourceRt")?;
        move || {
            runtime.block_on(async move {
                let (latest_stored_slot, db_ready_fut) = match db
                    .get_state_slot_info()
                    .context("failed to get state slot")?
                {
                    Some(value) => {
                        info!(slot = value.slot, "latest stored slot (db)");
                        (value, ready(Ok(db)).boxed())
                    }
                    None => {
                        let value =
                            source::rpc::get_confirmed_slot(config.state_init.endpoint.clone())
                                .await
                                .context("failed to get confirmed slot")?;
                        info!(slot = value.slot, "latest stored slot (rpc)");

                        let config = config.state_init;
                        let shutdown = shutdown.clone();
                        let fetch_state_fut = async move {
                            let sst_files = source::rpc::fetch_confirmed_state(
                                &db,
                                value.slot,
                                &config,
                                shutdown.clone(),
                            )
                            .await?;
                            if !shutdown.is_cancelled()
                                && let Err(error) = db.sst_ingest(sst_files, value)
                            {
                                db.destroy();
                                return Err(error).context("failed to consume sst files");
                            }
                            Ok::<_, anyhow::Error>(db)
                        };

                        (value, fetch_state_fut.boxed())
                    }
                };

                let (update_tx, update_rx) = mpsc::channel(config.banks.updates_channel_size);
                tokio::try_join!(
                    source::grpc::subscribe(
                        update_tx,
                        config.source,
                        latest_stored_slot.slot + 1,
                        shutdown.clone()
                    ),
                    blocks::start(db_ready_fut, latest_stored_slot, update_rx, shutdown)
                )?;

                Ok::<(), anyhow::Error>(())
            })
        }
    })?;
    threads.push(("socSource".to_owned(), Some(jh)));

    // Shutdown loop
    let mut signals = Signals::new([SIGINT, SIGTERM])?;
    'outer: while threads.iter().any(|th| th.1.is_some()) {
        for signal in signals.pending() {
            match signal {
                SIGINT | SIGTERM => {
                    if shutdown.is_cancelled() {
                        warn!("signal received again, shutdown now");
                        break 'outer;
                    }
                    info!("signal received, shutting down...");
                    shutdown.cancel();
                }
                _ => unreachable!(),
            }
        }

        for (name, tjh) in threads.iter_mut() {
            if let Some(jh) = tjh.take() {
                if jh.is_finished() {
                    jh.join()
                        .unwrap_or_else(|_| panic!("{name} thread join failed"))?;
                    info!("thread {name} finished");
                } else {
                    *tjh = Some(jh);
                }
            }
        }

        sleep(Duration::from_millis(25));
    }

    Ok(())
}
