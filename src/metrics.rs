use {
    anyhow::Context,
    metrics::{counter, describe_counter},
    metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle},
    richat_shared::jsonrpc::metrics::{
        RPC_REQUESTS_DURATION_SECONDS, describe as describe_jsonrpc_metrics,
    },
    std::net::SocketAddr,
    tokio::{
        task::JoinError,
        time::{Duration, sleep},
    },
};

pub fn setup() -> anyhow::Result<PrometheusHandle> {
    let default_buckets = &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(RPC_REQUESTS_DURATION_SECONDS.to_owned()),
            default_buckets,
        )?
        .install_recorder()
        .context("failed to install prometheus exporter")?;

    describe_jsonrpc_metrics();

    describe_counter!("version", "Socotra version info");
    counter!(
        "version",
        "buildts" => env!("VERGEN_BUILD_TIMESTAMP"),
        "git" => env!("GIT_VERSION"),
        "package" => env!("CARGO_PKG_NAME"),
        "richat" => env!("RICHAT_PROTO_VERSION"),
        "rustc" => env!("VERGEN_RUSTC_SEMVER"),
        "solana" => env!("SOLANA_SDK_VERSION"),
        "version" => env!("CARGO_PKG_VERSION"),
    )
    .absolute(1);

    Ok(handle)
}

pub async fn spawn_server(
    endpoint: SocketAddr,
    handle: PrometheusHandle,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
    let recorder_handle = handle.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;
            recorder_handle.run_upkeep();
        }
    });

    richat_metrics::spawn_server(
        richat_metrics::ConfigMetrics { endpoint },
        move || handle.render().into_bytes(), // metrics
        || true,                              // health
        move || true,                         // ready
        shutdown,
    )
    .await
    .map_err(Into::into)
}
