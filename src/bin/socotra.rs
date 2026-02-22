use {
    anyhow::Context,
    clap::Parser,
    futures::future::{FutureExt, TryFutureExt},
    opentelemetry::trace::TracerProvider,
    opentelemetry_otlp::WithExportConfig,
    signal_hook::{
        consts::{SIGINT, SIGTERM},
        iterator::Signals,
    },
    socotra::{
        config::Config,
        metrics, rpc, source,
        storage::{blocks, reader::Reader, rocksdb::Rocksdb},
    },
    std::{
        future::ready,
        io::{self, IsTerminal},
        thread::{self, sleep},
        time::Duration,
    },
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
    tracing::{info, level_filters::LevelFilter, warn},
    tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt},
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

    // Setup monitoring
    let otel_runtime = setup_tracing(
        config.monitoring.logs_json,
        config.monitoring.otlp_endpoint.as_deref(),
    )?;
    let metrics_handle = metrics::setup()?;

    // Exit if we only check the config
    if args.check {
        info!("Config is OK!");
        return Ok(());
    }

    // Shutdown
    let mut threads = Vec::<(String, _)>::with_capacity(2);
    let shutdown = CancellationToken::new();

    // Open storage
    let storage_init_config = config.storage.init;
    let storage_blocks_config = config.storage.blocks;
    let db = Rocksdb::open(config.storage.path, config.storage.compression)?;
    let (reader, reader_threads) = Reader::new(
        db.clone(),
        storage_blocks_config.request_channel_capacity,
        storage_blocks_config.read_workers,
        config.rpc.request_timeout,
    )?;
    for th in reader_threads {
        let name = th
            .thread()
            .name()
            .map(|name| name.to_owned())
            .unwrap_or_default();
        threads.push((name, Some(th)));
    }

    // Source
    let jh = thread::Builder::new().name("socSource".to_owned()).spawn({
        let runtime = config.source.tokio.clone().build_runtime("socSourceRt")?;
        let reader = reader.clone();
        let shutdown = shutdown.clone();
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
                            source::rpc::get_confirmed_slot(storage_init_config.endpoint.clone())
                                .await
                                .context("failed to get confirmed slot")?;
                        info!(slot = value.slot, "latest stored slot (rpc)");

                        let config = storage_init_config;
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

                let (geyser_update_tx, geyser_update_rx) =
                    mpsc::channel(storage_blocks_config.updates_channel_size);
                let _: ((), (), ()) = tokio::try_join!(
                    source::grpc::subscribe(
                        geyser_update_tx,
                        config.source,
                        latest_stored_slot.slot + 1,
                        shutdown.clone()
                    ),
                    blocks::start(
                        db_ready_fut,
                        latest_stored_slot,
                        geyser_update_rx,
                        reader,
                        shutdown.clone()
                    ),
                    metrics::spawn_server(
                        config.monitoring.prometheus_endpoint,
                        metrics_handle,
                        shutdown.cancelled_owned(),
                    )
                    .await?
                    .map_err(anyhow::Error::from)
                    .boxed(),
                )?;

                Ok::<(), anyhow::Error>(())
            })
        }
    })?;
    threads.push(("socSource".to_owned(), Some(jh)));

    // RPC
    let jh = thread::Builder::new().name("socRpc".to_owned()).spawn({
        let shutdown = shutdown.clone();
        move || {
            let runtime = config.rpc.tokio.clone().build_runtime("socRpcRt")?;
            runtime.block_on(async move {
                let _: () = rpc::server::spawn(config.rpc, reader, shutdown).await?;
                Ok::<(), anyhow::Error>(())
            })
        }
    })?;
    threads.push(("socRpc".to_owned(), Some(jh)));

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

    // Flush remaining OTLP spans while the runtime is still alive
    if let Some(otel) = otel_runtime {
        let _guard = otel.runtime.enter();
        let _ = otel.tracer_provider.shutdown();
    }

    Ok(())
}

struct OtelState {
    runtime: tokio::runtime::Runtime,
    tracer_provider: opentelemetry_sdk::trace::SdkTracerProvider,
}

fn setup_tracing(
    logs_json: bool,
    otlp_endpoint: Option<&str>,
) -> anyhow::Result<Option<OtelState>> {
    // Always add a console layer
    let io_layer = tracing_subscriber::fmt::layer().with_line_number(true);
    let io_layer_ansi = if logs_json {
        io_layer.json().boxed()
    } else {
        let is_atty = io::stdout().is_terminal() && io::stderr().is_terminal();
        io_layer.with_ansi(is_atty).boxed()
    };

    // Filter directives
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    // Optional OTLP layer for exporting spans via gRPC.
    // The batch exporter spawns a Tokio task, so we need a runtime.
    // The runtime must outlive the tracer provider, so we return it to the caller.
    let (otel_layer, otel_state) = if let Some(endpoint) = otlp_endpoint {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .thread_name("socOtel")
            .build()
            .context("failed to create OTLP runtime")?;
        let _guard = runtime.enter();

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()?;

        let batch_processor = opentelemetry_sdk::trace::BatchSpanProcessor::builder(exporter)
            .with_batch_config(
                opentelemetry_sdk::trace::BatchConfigBuilder::default()
                    // Internal buffer capacity (default: 2048). Spans are dropped when full.
                    .with_max_queue_size(65536)
                    // Spans per gRPC export call (default: 512). Larger batches = fewer RPCs.
                    .with_max_export_batch_size(4096)
                    // Flush interval (default: 5s). Shorter delay drains the queue faster.
                    .with_scheduled_delay(Duration::from_secs(1))
                    .build(),
            )
            .build();

        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_span_processor(batch_processor)
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name("socotra")
                    .build(),
            )
            .build();

        opentelemetry::global::set_tracer_provider(tracer_provider.clone());

        let tracer = tracer_provider.tracer("socotra");
        (
            Some(tracing_opentelemetry::layer().with_tracer(tracer)),
            Some(OtelState {
                runtime,
                tracer_provider,
            }),
        )
    } else {
        (None, None)
    };

    // Construct the subscriber with all layers
    tracing_subscriber::registry()
        .with(env_filter)
        .with(io_layer_ansi)
        .with(otel_layer)
        .try_init()
        .context("failed to set global default subscriber")?;

    Ok(otel_state)
}
