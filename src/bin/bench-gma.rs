use {
    clap::Parser,
    indicatif::{ProgressBar, ProgressStyle},
    solana_account_decoder::UiAccountEncoding,
    solana_commitment_config::CommitmentConfig,
    solana_rpc_client::{
        api::config::RpcAccountInfoConfig, nonblocking::rpc_client::RpcClient,
    },
    futures::future::join_all,
    solana_sdk::pubkey::Pubkey,
    std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    },
};

fn parse_encoding(s: &str) -> Result<UiAccountEncoding, String> {
    match s {
        "base64" => Ok(UiAccountEncoding::Base64),
        "base64+zstd" => Ok(UiAccountEncoding::Base64Zstd),
        "base58" => Ok(UiAccountEncoding::Base58),
        "jsonParsed" => Ok(UiAccountEncoding::JsonParsed),
        _ => Err(format!("unknown encoding: {s}")),
    }
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:9000")]
    endpoint: String,

    #[arg(long)]
    pubkeys: PathBuf,

    #[arg(long, default_value_t = 10)]
    concurrency: usize,

    #[arg(long, default_value = "finalized")]
    commitment: CommitmentConfig,

    #[arg(long, default_value = "base64", value_parser = parse_encoding)]
    encoding: UiAccountEncoding,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let pubkeys_json = tokio::fs::read_to_string(&args.pubkeys).await?;
    let pubkeys: Vec<Pubkey> = serde_json::from_str::<Vec<String>>(&pubkeys_json)?
        .iter()
        .map(|s| s.parse())
        .collect::<Result<_, _>>()?;

    let config = RpcAccountInfoConfig {
        encoding: Some(args.encoding),
        commitment: Some(args.commitment),
        ..Default::default()
    };

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {pos} accounts | {elapsed_precise} | errors: {msg}")
            .unwrap(),
    );
    pb.enable_steady_tick(Duration::from_millis(100));
    pb.set_message("0");

    let client = Arc::new(RpcClient::new(args.endpoint));
    let errors = Arc::new(AtomicU64::new(0));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(args.concurrency));

    let mut handles = Vec::new();
    for chunk in pubkeys.chunks(100) {
        let chunk = chunk.to_vec();
        let semaphore = Arc::clone(&semaphore);
        let client = Arc::clone(&client);
        let config = config.clone();
        let errors = Arc::clone(&errors);
        let pb = pb.clone();

        handles.push(tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let result = client
                .get_multiple_ui_accounts_with_config(&chunk, config)
                .await;

            if result.is_err() {
                let count = errors.fetch_add(1, Ordering::Relaxed) + 1;
                pb.set_message(count.to_string());
            }

            pb.inc(chunk.len() as u64);
        }));
    }

    join_all(handles).await;
    pb.finish();

    Ok(())
}
