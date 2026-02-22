use {
    anyhow::Context,
    metrics::{counter, describe_counter, describe_histogram},
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

pub const WRITE_BLOCK_SYNC_SECONDS: &str = "write_block_sync_seconds";
pub const BUILD_READER_STATE_SECONDS: &str = "build_reader_state_seconds";

pub fn setup() -> anyhow::Result<PrometheusHandle> {
    let default_buckets = &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(RPC_REQUESTS_DURATION_SECONDS.to_owned()),
            default_buckets,
        )?
        .set_buckets_for_metric(
            Matcher::Full(WRITE_BLOCK_SYNC_SECONDS.to_owned()),
            &[0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1, 0.2, 0.4],
        )?
        .set_buckets_for_metric(
            Matcher::Full(BUILD_READER_STATE_SECONDS.to_owned()),
            &[0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1],
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

    describe_histogram!(WRITE_BLOCK_SYNC_SECONDS, "Write block sync time");
    describe_histogram!(BUILD_READER_STATE_SECONDS, "Build reader state time");

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
