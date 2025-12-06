use {
    anyhow::Context,
    clap::Parser,
    futures::stream::StreamExt,
    maplit::hashmap,
    richat_client::grpc::ConfigGrpcClient,
    richat_proto::geyser::{
        CommitmentLevel as CommitmentLevelProto, SlotStatus, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeUpdate,
        SubscribeUpdateAccount, SubscribeUpdateSlot, subscribe_update::UpdateOneof,
    },
    socotra::rocksdb::{AccountIndexValue, Rocksdb},
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    std::{
        collections::{BTreeMap, HashMap},
        io::{self, IsTerminal},
        time::Instant,
    },
    tracing::{error, info},
    tracing_subscriber::{
        filter::{EnvFilter, LevelFilter},
        fmt::layer,
        layer::SubscriberExt,
        util::SubscriberInitExt,
    },
};

#[derive(Debug, Parser)]
#[clap(author, version, about = "TODO")]
struct Args {
    /// Yellowstone gRPC endpoint
    #[clap(long)]
    pub endpoint: String,

    /// Yellowstone gRPC x-token
    #[clap(long)]
    pub x_token: Option<String>,
}

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

    let location_db = "/home/ubuntu/storage/accounts-db/db";

    let db = Rocksdb::open(location_db)?;

    let args = Args::parse();

    let config = ConfigGrpcClient {
        endpoint: args.endpoint,
        x_token: args.x_token.map(|token| token.into_bytes()),
        max_decoding_message_size: 16 * 1024 * 1024,
        ..Default::default()
    };
    let mut connection = config.connect().await?;

    let version = connection.get_version().await?;
    info!(version = version.version, "connected to stream");

    let mut stream = connection
        .subscribe_dragons_mouth_once(SubscribeRequest {
            accounts: hashmap! { "".to_owned() => SubscribeRequestFilterAccounts::default() },
            slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            } },
            transactions: hashmap! {},
            transactions_status: hashmap! {},
            blocks: hashmap! {},
            blocks_meta: hashmap! {},
            entry: hashmap! {},
            commitment: Some(CommitmentLevelProto::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        })
        .await?
        .into_parsed();

    let mut map = BTreeMap::<Slot, HashMap<Pubkey, (u64, AccountIndexValue)>>::new();
    loop {
        let message = match stream.next().await {
            Some(Ok(SubscribeUpdate {
                update_oneof: Some(message),
                ..
            })) => message,
            Some(Ok(SubscribeUpdate {
                update_oneof: None, ..
            })) => {
                error!("no update in the message");
                break;
            }
            Some(Err(error)) => {
                error!(?error, "failed to get next message");
                break;
            }
            None => {
                error!("stream is finished");
                break;
            }
        };

        match message {
            UpdateOneof::Slot(SubscribeUpdateSlot { slot, status, .. })
                if status == SlotStatus::SlotConfirmed as i32 =>
            {
                if let Some(accounts) = map.remove(&slot) {
                    let len = accounts.len();
                    let ts = Instant::now();
                    db.push_accounts(
                        accounts
                            .into_iter()
                            .map(|(pubkey, (_write_version, value))| (pubkey, value)),
                    )?;
                    info!(slot, updates = len, elapsed = ?ts.elapsed(), "add accounts for confirmed slot");
                }

                loop {
                    match map.keys().next().copied() {
                        Some(slot_min) if slot_min <= slot => {
                            map.remove(&slot_min);
                        }
                        _ => break,
                    }
                }
            }
            UpdateOneof::Account(SubscribeUpdateAccount {
                slot,
                account: Some(account),
                ..
            }) => {
                let pubkey = Pubkey::try_from(account.pubkey.as_slice())
                    .context("failed to create pubkey")?;

                let entry = map.entry(slot).or_default().entry(pubkey).or_default();
                if entry.0 < account.write_version {
                    entry.0 = account.write_version;
                    entry.1 = AccountIndexValue {
                        lamports: account.lamports,
                        owner: Pubkey::try_from(account.owner.as_slice())
                            .context("failed to create owner")?,
                        data: account.data,
                        executable: account.executable,
                        rent_epoch: account.rent_epoch,
                    };
                }
            }
            _ => {}
        }
    }

    Ok(())
}
