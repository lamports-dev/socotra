use {
    agave_feature_set::{FEATURE_NAMES, ID},
    anyhow::Context,
    clap::Parser,
    solana_rpc_client::{api::request::MAX_MULTIPLE_ACCOUNTS, nonblocking::rpc_client::RpcClient},
    solana_sdk::{epoch_schedule::EpochSchedule, pubkey::Pubkey},
    std::collections::BTreeMap,
};

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value = "https://api.mainnet-beta.solana.com")]
    endpoint: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let client = RpcClient::new(args.endpoint);

    let epoch_schedule: EpochSchedule = {
        let account = client
            .get_account(&solana_sdk_ids::sysvar::epoch_schedule::id())
            .await
            .context("failed to get epoch schedule sysvar")?;
        bincode::deserialize(&account.data).context("failed to deserialize epoch schedule")?
    };

    let mut active = BTreeMap::new();
    let mut inactive = BTreeMap::new();

    let mut iter = FEATURE_NAMES.iter();
    loop {
        let (pubkeys, names): (Vec<Pubkey>, Vec<&str>) =
            iter.by_ref().take(MAX_MULTIPLE_ACCOUNTS).unzip();
        if pubkeys.is_empty() {
            break;
        }

        let accounts = client
            .get_multiple_accounts(&pubkeys)
            .await
            .context("failed to get features with gMA")?;

        for (idx, maybe_account) in accounts.into_iter().enumerate() {
            let pubkey = pubkeys[idx].to_string();
            let name = names[idx];

            if let Some(account) = maybe_account
                && let Some(slot) = parse_activation_slot(&account.data)
            {
                active.insert(pubkey, (slot, name));
                continue;
            }
            inactive.insert(pubkey, name);
        }
    }

    let mut active = active
        .into_iter()
        .map(|(pubkey, (slot, name))| (slot, (pubkey, name)))
        .collect::<Vec<_>>();
    active.sort_unstable();

    let feature_set = u32::from_le_bytes(
        ID.as_ref()[..4]
            .try_into()
            .expect("hash length is verified"),
    );

    println!(
        "feature_set: # agave v{} / feature set: {feature_set}",
        env!("AGAVE_FEATURE_SET_VERSION")
    );
    let max_pubkey_len = max_len(inactive.keys());
    println!("  inactive:");
    for (pubkey, name) in &inactive {
        println!("    - {pubkey:<max_pubkey_len$} # {name}");
    }

    let active_entries: Vec<_> = active
        .iter()
        .map(|(slot, (pubkey, name))| {
            let epoch = epoch_schedule.get_epoch(*slot);
            let slot_info = format!("from slot {slot} (epoch {epoch})");
            (pubkey, slot_info, *name)
        })
        .collect();
    let max_pubkey_len = max_len(active_entries.iter().map(|(p, _, _)| p));
    let max_slot_info_len = max_len(active_entries.iter().map(|(_, s, _)| s));
    println!("  active:");
    for (pubkey, slot_info, name) in &active_entries {
        println!("    - {pubkey:<max_pubkey_len$} # {slot_info:<max_slot_info_len$} | {name}",);
    }

    Ok(())
}

fn parse_activation_slot(data: &[u8]) -> Option<u64> {
    if data.is_empty() || data[0] == 0 {
        return None;
    }
    if data.len() >= 9 {
        Some(u64::from_le_bytes(
            data[1..9].try_into().expect("length already verified"),
        ))
    } else {
        None
    }
}

fn max_len<S: AsRef<str>>(iter: impl Iterator<Item = S>) -> usize {
    iter.map(|s| s.as_ref().len()).max().unwrap_or(0)
}
