use {
    crate::{
        config::ConfigStateInit,
        rocksdb::{Rocksdb, SlotIndexValue},
    },
    anyhow::Context as _,
    bytes::{Bytes, BytesMut},
    futures::{StreamExt, future::try_join_all, stream::Stream},
    pin_project::pin_project,
    reqwest::{Client, Url, header::CONTENT_TYPE},
    rocksdb::{DBCompressionType, SstFileWriter},
    solana_commitment_config::CommitmentConfig,
    solana_rpc_client::{api::config::RpcBlockConfig, nonblocking::rpc_client::RpcClient},
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    solana_transaction_status_client_types::{TransactionDetails, UiTransactionEncoding},
    std::{
        path::{Path, PathBuf},
        pin::Pin,
        task::{Context, Poll},
        time::Instant,
    },
    tokio_util::sync::CancellationToken,
    tracing::info,
};

pub async fn get_confirmed_slot(endpoint: String) -> anyhow::Result<SlotIndexValue> {
    let rpc = RpcClient::new(endpoint);

    let slot = rpc
        .get_slot_with_commitment(CommitmentConfig::confirmed())
        .await
        .context("failed to get confirmed slot")?;
    let height = rpc
        .get_block_with_config(
            slot,
            RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                transaction_details: Some(TransactionDetails::None),
                rewards: Some(false),
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(u8::MAX),
            },
        )
        .await
        .context("failed to get confirmed block")?
        .block_height
        .context("no height in received confirmed block")?;

    Ok(SlotIndexValue { slot, height })
}

/// Fetches confirmed account state from a patched RPC endpoint and writes SST files.
///
/// # Arguments
/// * `output` - Directory path where SST files will be written
/// * `endpoint` - RPC endpoint URL with get-account-state patch
/// * `segments` - Number of parallel segments to fetch (must be power of two, max 64)
///
/// # Returns
/// Vector of paths to created SST files
pub async fn fetch_confirmed_state(
    slot: Slot,
    config: &ConfigStateInit,
    shutdown: CancellationToken,
) -> anyhow::Result<Vec<PathBuf>> {
    anyhow::ensure!(
        config.segments.is_power_of_two() && config.segments <= 64,
        "segments must be a power of two and <= 64"
    );

    tokio::fs::create_dir_all(&config.path)
        .await
        .context("failed to create output directory")?;

    let url = Url::parse(&config.endpoint)
        .context("failed to parse endpoint")?
        .join(&format!("/get-account-state/{slot}"))
        .context("failed to create base endpoint")?;

    // segments is power of two, so we can use leading bits to divide the range
    // e.g., segments=4 means we split by first 2 bits: [0x00.., 0x40.., 0x80.., 0xC0..]
    // segments=64 means first 6 bits: [0x00.., 0x04.., 0x08.., ..., 0xFC..]
    let shift = 8 - config.segments.trailing_zeros() as u8; // bits to shift within first byte

    let sst_files: Vec<_> = (0..config.segments)
        .map(|segment| config.path.join(format!("{segment:03}.sst")))
        .collect();

    let ts = Instant::now();
    let mut handles: Vec<_> = sst_files
        .iter()
        .enumerate()
        .map(|(segment, file)| {
            let compression = config.compression.into();
            let url = url.clone();
            let file = file.clone();
            tokio::spawn(async move {
                fetch_segment(file, compression, url, segment as u8, shift).await
            })
        })
        .collect();

    tokio::select! {
        _ = shutdown.cancelled() => {
            for handle in handles {
                handle.abort();
            }
            for file in sst_files {
                let _ = tokio::fs::remove_file(file).await;
            }
            Ok(vec![])
        }
        result = try_join_all(&mut handles) => {
            result?
                .into_iter()
                .collect::<anyhow::Result<Vec<_>>>().context("failed to fetch segment")?;
            info!(elapsed = ?ts.elapsed(), "full stated fetched");
            Ok(sst_files)
        }
    }
}

async fn fetch_segment<P>(
    output: P,
    compression: DBCompressionType,
    mut url: Url,
    segment: u8,
    shift: u8,
) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    let ts = Instant::now();

    let start_byte = segment << shift;
    let mut start = [0u8; 32];
    start[0] = start_byte;
    let start = Pubkey::from(start);
    let mut end = [0xFFu8; 32];
    end[0] = start_byte | ((1u8 << shift) - 1);
    let end = Pubkey::from(end);

    url.query_pairs_mut()
        .append_pair("start", &start.to_string())
        .append_pair("end", &end.to_string());

    let stream = Client::new()
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .body("[]")
        .send()
        .await
        .context("failed to send request")?
        .bytes_stream();
    let mut accounts = AccountsStream::new(stream);

    let options = Rocksdb::get_sst_options(compression);
    let mut sst = SstFileWriter::create(&options);
    sst.open(output)?;
    loop {
        match accounts.next().await {
            Some(Ok((pubkey, account))) => sst.put(pubkey, account)?,
            Some(Err(error)) => return Err(error.into()),
            None => break,
        }
    }
    sst.finish()?;

    info!(segment, %start, %end, elapsed = ?ts.elapsed(), "partial state fetched");
    Ok(())
}

#[pin_project]
struct AccountsStream<S> {
    #[pin]
    inner: S,
    buf: BytesMut,
    cursor: usize,
    state: AccountsStreamState,
    finished: bool,
}

impl<S> AccountsStream<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            buf: BytesMut::with_capacity(16 * 1024 * 1024), // 16 MiB
            cursor: 0,
            state: AccountsStreamState::Pubkey,
            finished: false,
        }
    }

    /// Try to decode one account from the buffer
    /// Returns None if more data is needed
    /// Returns (pubkey_bytes, serialized_account_value) where account_value is in the same wire format
    fn try_decode_account_from(
        buf: &mut BytesMut,
        cursor: &mut usize,
        state: &mut AccountsStreamState,
    ) -> Option<Result<(BytesMut, BytesMut), AccountsStreamDecodeError>> {
        let remaining = &buf[*cursor..];

        loop {
            match *state {
                AccountsStreamState::Pubkey => {
                    if remaining.len() < 32 {
                        return None;
                    }
                    *cursor += 32;
                    *state = AccountsStreamState::Lamports;
                }
                AccountsStreamState::Lamports => {
                    let remaining = &buf[*cursor..];
                    let (_, bytes_read) = match Self::peek_varint(remaining) {
                        Ok(Some(v)) => v,
                        Ok(None) => return None,
                        Err(error) => return Some(Err(error)),
                    };
                    *cursor += bytes_read;
                    *state = AccountsStreamState::DataLength;
                }
                AccountsStreamState::DataLength => {
                    let remaining = &buf[*cursor..];
                    let (data_len, bytes_read) = match Self::peek_varint(remaining) {
                        Ok(Some(v)) => v,
                        Ok(None) => return None,
                        Err(error) => return Some(Err(error)),
                    };
                    *cursor += bytes_read;
                    *state = AccountsStreamState::Data {
                        data_len: data_len as usize,
                    };
                }
                AccountsStreamState::Data { data_len } => {
                    let remaining = &buf[*cursor..];
                    if remaining.len() < data_len {
                        return None;
                    }
                    *cursor += data_len;
                    *state = AccountsStreamState::Owner;
                }
                AccountsStreamState::Owner => {
                    let remaining = &buf[*cursor..];
                    if remaining.len() < 32 {
                        return None;
                    }
                    *cursor += 32;
                    *state = AccountsStreamState::Executable;
                }
                AccountsStreamState::Executable => {
                    let remaining = &buf[*cursor..];
                    if remaining.is_empty() {
                        return None;
                    }
                    *cursor += 1;
                    *state = AccountsStreamState::RentEpoch;
                }
                AccountsStreamState::RentEpoch => {
                    let remaining = &buf[*cursor..];
                    let (_, bytes_read) = match Self::peek_varint(remaining) {
                        Ok(Some(v)) => v,
                        Ok(None) => return None,
                        Err(error) => return Some(Err(error)),
                    };
                    *cursor += bytes_read;

                    // Split out completed account
                    let mut account_bytes = buf.split_to(*cursor);
                    *cursor = 0;
                    *state = AccountsStreamState::Pubkey;

                    let pubkey = account_bytes.split_to(32);
                    return Some(Ok((pubkey, account_bytes)));
                }
            }
        }
    }

    /// Peek at a varint without consuming the buffer
    /// Returns (value, bytes_consumed), None if incomplete, or Err for overflow
    fn peek_varint(buf: &[u8]) -> Result<Option<(u64, usize)>, AccountsStreamDecodeError> {
        let mut result: u64 = 0;
        let mut shift = 0;
        let mut i = 0;

        loop {
            if i >= buf.len() {
                return Ok(None); // Need more data
            }

            let byte = buf[i];
            i += 1;

            result |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                return Ok(Some((result, i)));
            }

            shift += 7;
            if shift >= 64 {
                return Err(AccountsStreamDecodeError::VarintOverflow);
            }
        }
    }
}

impl<S> Stream for AccountsStream<S>
where
    S: Stream<Item = reqwest::Result<Bytes>>,
{
    type Item = Result<(BytesMut, BytesMut), AccountsStreamDecodeError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.finished {
            return Poll::Ready(None);
        }

        loop {
            // Try to decode accounts from buffer
            if let Some(result) = Self::try_decode_account_from(this.buf, this.cursor, this.state) {
                return Poll::Ready(Some(result));
            }

            // Read more data from stream
            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    this.buf.extend_from_slice(&chunk);
                }
                Poll::Ready(Some(Err(e))) => {
                    *this.finished = true;
                    return Poll::Ready(Some(Err(AccountsStreamDecodeError::NetworkError(e))));
                }
                Poll::Ready(None) => {
                    *this.finished = true;
                    return if *this.state == AccountsStreamState::Pubkey {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(Err(AccountsStreamDecodeError::InsufficientData)))
                    };
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccountsStreamState {
    Pubkey,
    Lamports,
    DataLength,
    Data { data_len: usize },
    Owner,
    Executable,
    RentEpoch,
}

#[derive(Debug, thiserror::Error)]
enum AccountsStreamDecodeError {
    #[error("Insufficient data in buffer")]
    InsufficientData,
    #[error("Varint overflow")]
    VarintOverflow,
    #[error("Network error: {0:?}")]
    NetworkError(reqwest::Error),
}
