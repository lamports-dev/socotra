use {
    crate::{
        config::ConfigRpc,
        storage::reader::{ReadResultAccount, ReadResultSlot, Reader},
    },
    ahash::HashMap,
    futures::future::BoxFuture,
    jsonrpsee_types::{
        Extensions, Id, Params, Request, Response, ResponsePayload, TwoPointZero,
        error::{ErrorCode, ErrorObject, ErrorObjectOwned, INVALID_PARAMS_MSG},
    },
    richat_shared::jsonrpc::{
        helpers::{
            jsonrpc_error_invalid_params, jsonrpc_response_error, jsonrpc_response_error_custom,
            jsonrpc_response_success, to_vec,
        },
        requests::{RpcRequestResult, RpcRequestsProcessor},
    },
    serde::{Deserialize, de},
    solana_account_decoder::{
        MAX_BASE58_BYTES, UiAccount, UiAccountEncoding, UiDataSliceConfig, encode_ui_account,
        parse_account_data::AccountAdditionalDataV3,
        parse_token::{get_token_account_mint, is_known_spl_token_id},
    },
    solana_commitment_config::CommitmentLevel,
    solana_rpc_client::api::{
        config::{RpcAccountInfoConfig, RpcContextConfig},
        custom_error::RpcCustomError,
        response::{Response as RpcResponse, RpcResponseContext, RpcVersionInfo},
    },
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::sync::Arc,
    tracing::{Instrument, info_span},
};

#[derive(Debug)]
pub struct State {
    reader: Reader,
}

pub fn create_request_processor(
    config: ConfigRpc,
    reader: Reader,
) -> RpcRequestsProcessor<Arc<State>> {
    let mut processor = RpcRequestsProcessor::new(
        config.body_limit,
        Arc::new(State { reader }),
        config.extra_headers,
    );

    processor.add_handler("getVersion", Box::new(RpcRequestVersion::handle));
    processor.add_handler("getSlot", Box::new(RpcRequestSlot::handle));
    processor.add_handler("getAccountInfo", Box::new(RpcRequestAccountInfo::handle));
    processor.add_handler(
        "getMultipleAccounts",
        Box::new(RpcRequestMultipleAccounts::handle),
    );

    processor
}

trait RpcRequestHandler: Sized {
    fn handle(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> BoxFuture<'_, RpcRequestResult>
    where
        Self: Send,
    {
        Box::pin(async move {
            let span = info_span!("rpc_request", method = request.method.as_ref());
            async move {
                let req = {
                    let _parse = info_span!("rpc_parse").entered();
                    Self::parse(state, x_subscription_id, upstream_disabled, request)
                };
                match req {
                    Ok(req) => req.process().instrument(info_span!("rpc_process")).await,
                    Err(response) => Ok(response),
                }
            }
            .instrument(span)
            .await
        })
    }

    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>>;

    fn process(self) -> impl Future<Output = RpcRequestResult> + Send {
        async { unimplemented!() }
    }
}

fn parse_params<'a, T>(request: Request<'a>) -> Result<(Id<'a>, T), Vec<u8>>
where
    T: for<'de> de::Deserialize<'de>,
{
    let params = Params::new(request.params.as_ref().map(|p| p.get()));
    match params.parse() {
        Ok(params) => Ok((request.id, params)),
        Err(error) => Err(to_vec(&Response {
            jsonrpc: Some(TwoPointZero),
            payload: ResponsePayload::<()>::error(error),
            id: request.id,
            extensions: Extensions::default(),
        })),
    }
}

fn no_params_expected(request: Request<'_>) -> Result<Request<'_>, Vec<u8>> {
    if let Some(error) = match serde_json::from_str::<serde_json::Value>(
        request.params.as_ref().map(|p| p.get()).unwrap_or("null"),
    ) {
        Ok(value) => match value {
            serde_json::Value::Null => None,
            serde_json::Value::Array(vec) if vec.is_empty() => None,
            value => Some(jsonrpc_error_invalid_params(
                "No parameters were expected",
                Some(value.to_string()),
            )),
        },
        Err(error) => Some(jsonrpc_error_invalid_params(
            INVALID_PARAMS_MSG,
            Some(error.to_string()),
        )),
    } {
        Err(jsonrpc_response_error(request.id, error))
    } else {
        Ok(request)
    }
}

#[derive(Debug)]
struct RpcRequestVersion;

impl RpcRequestHandler for RpcRequestVersion {
    fn parse(
        _state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        let request = no_params_expected(request)?;
        let version = solana_version::Version::default();
        Err(jsonrpc_response_success(
            request.id,
            serde_json::json!(RpcVersionInfo {
                solana_core: version.to_string(),
                feature_set: Some(version.feature_set),
            }),
        ))
    }
}

#[derive(Debug)]
struct RpcRequestSlot {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    commitment: CommitmentLevel,
    min_context_slot: Option<Slot>,
}

impl RpcRequestHandler for RpcRequestSlot {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (id, ReqParams { config }) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
        let RpcContextConfig {
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default().commitment;

        Ok(Self {
            state,
            x_subscription_id,
            id: id.into_owned(),
            commitment,
            min_context_slot,
        })
    }

    async fn process(self) -> RpcRequestResult {
        match self
            .state
            .reader
            .get_slot(
                self.x_subscription_id,
                self.commitment,
                self.min_context_slot,
            )
            .await
        {
            ReadResultSlot::ReqChanClosed => anyhow::bail!("reader workers terminated"),
            ReadResultSlot::ReqChanFull => anyhow::bail!("request queue is full"),
            ReadResultSlot::ReqDrop => anyhow::bail!("request dropped by reader"),
            ReadResultSlot::Timeout => anyhow::bail!("request timeout"),
            ReadResultSlot::MinContextSlotNotReached { context_slot } => {
                Ok(jsonrpc_response_error_custom(
                    self.id,
                    RpcCustomError::MinContextSlotNotReached { context_slot },
                ))
            }
            ReadResultSlot::Slot(slot) => Ok(jsonrpc_response_success(self.id, slot)),
        }
    }
}

#[derive(Debug)]
struct RpcRequestAccountInfo(RpcRequestAccounts);

impl RpcRequestHandler for RpcRequestAccountInfo {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            pubkey_str: String,
            #[serde(default)]
            config: Option<RpcAccountInfoConfig>,
        }

        let (id, ReqParams { pubkey_str, config }) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
        let pubkey = match verify_pubkey(&pubkey_str) {
            Ok(pubkey) => pubkey,
            Err(error) => return Err(jsonrpc_response_error(id, error)),
        };
        Ok(RpcRequestAccountInfo(RpcRequestAccounts::new(
            state,
            x_subscription_id,
            id.into_owned(),
            false,
            vec![pubkey],
            config,
        )))
    }

    async fn process(self) -> RpcRequestResult {
        self.0.process().await
    }
}

#[derive(Debug)]
struct RpcRequestMultipleAccounts(RpcRequestAccounts);

impl RpcRequestHandler for RpcRequestMultipleAccounts {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            pubkey_strs: Vec<String>,
            #[serde(default)]
            config: Option<RpcAccountInfoConfig>,
        }

        let (
            id,
            ReqParams {
                pubkey_strs,
                config,
            },
        ) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
        let pubkeys = match pubkey_strs
            .into_iter()
            .map(|pubkey_str| verify_pubkey(&pubkey_str))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(pubkeys) => pubkeys,
            Err(error) => {
                return Err(jsonrpc_response_error(id, error));
            }
        };
        Ok(RpcRequestMultipleAccounts(RpcRequestAccounts::new(
            state,
            x_subscription_id,
            id.into_owned(),
            true,
            pubkeys,
            config,
        )))
    }

    async fn process(self) -> RpcRequestResult {
        self.0.process().await
    }
}

#[derive(Debug)]
struct RpcRequestAccounts {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    multiple: bool,
    pubkeys: Vec<Pubkey>,
    commitment: CommitmentLevel,
    min_context_slot: Option<Slot>,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
}

impl RpcRequestAccounts {
    fn new(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        id: Id<'static>,
        multiple: bool,
        pubkeys: Vec<Pubkey>,
        config: Option<RpcAccountInfoConfig>,
    ) -> Self {
        let RpcAccountInfoConfig {
            encoding,
            data_slice,
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default().commitment;
        let encoding = encoding.unwrap_or(UiAccountEncoding::Binary);

        Self {
            state,
            x_subscription_id,
            id,
            multiple,
            pubkeys,
            commitment,
            min_context_slot,
            encoding,
            data_slice,
        }
    }

    async fn process(self) -> RpcRequestResult {
        match self
            .state
            .reader
            .get_accounts(
                self.x_subscription_id,
                self.pubkeys,
                self.commitment,
                self.min_context_slot,
                self.encoding == UiAccountEncoding::JsonParsed,
            )
            .await
        {
            ReadResultAccount::ReqChanClosed => anyhow::bail!("reader workers terminated"),
            ReadResultAccount::ReqChanFull => anyhow::bail!("request queue is full"),
            ReadResultAccount::ReqDrop => anyhow::bail!("request dropped by reader"),
            ReadResultAccount::Timeout => anyhow::bail!("request timeout"),
            ReadResultAccount::MinContextSlotNotReached { context_slot } => {
                Ok(jsonrpc_response_error_custom(
                    self.id,
                    RpcCustomError::MinContextSlotNotReached { context_slot },
                ))
            }
            ReadResultAccount::TokenMintUnpackFailed => Ok(jsonrpc_response_error(
                self.id,
                jsonrpc_error_invalid_params::<()>(
                    "Invalid param: Token mint could not be unpacked".to_owned(),
                    None,
                ),
            )),
            ReadResultAccount::RequestFailed(error) => {
                anyhow::bail!("request to db failed: {error}")
            }
            ReadResultAccount::Accounts {
                slot,
                pubkeys,
                accounts,
                mints,
            } => Ok(
                match pubkeys
                    .into_iter()
                    .zip(accounts.into_iter())
                    .map(|(pubkey, account)| {
                        encode_account_token(
                            account,
                            pubkey,
                            self.encoding,
                            self.data_slice,
                            &mints,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(mut value) => {
                        let context = RpcResponseContext::new(slot);
                        if self.multiple {
                            jsonrpc_response_success(self.id, &RpcResponse { context, value })
                        } else {
                            let value = value.pop();
                            jsonrpc_response_success(self.id, &RpcResponse { context, value })
                        }
                    }
                    Err(error) => jsonrpc_response_error(self.id, error),
                },
            ),
        }
    }
}

fn verify_pubkey(input: &str) -> Result<Pubkey, ErrorObjectOwned> {
    input.parse().map_err(|error| {
        jsonrpc_error_invalid_params::<()>(format!("Invalid param: {error:?}"), None)
    })
}

fn encode_account_token(
    account: Option<Arc<Account>>,
    pubkey: Pubkey,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
    mints: &HashMap<Pubkey, AccountAdditionalDataV3>,
) -> Result<Option<UiAccount>, ErrorObjectOwned> {
    account
        .map(|account| {
            if is_known_spl_token_id(&account.owner) && encoding == UiAccountEncoding::JsonParsed {
                let additional_data = get_token_account_mint(&account.data)
                    .and_then(|mint_pubkey| mints.get(&mint_pubkey))
                    .cloned();
                Ok(encode_ui_account(
                    &pubkey,
                    &account,
                    UiAccountEncoding::JsonParsed,
                    additional_data,
                    None,
                ))
            } else {
                encode_account(&account, &pubkey, encoding, data_slice)
            }
        })
        .transpose()
}

fn encode_account(
    account: &Account,
    pubkey: &Pubkey,
    encoding: UiAccountEncoding,
    data_slice: Option<UiDataSliceConfig>,
) -> Result<UiAccount, ErrorObjectOwned> {
    if (encoding == UiAccountEncoding::Binary || encoding == UiAccountEncoding::Base58)
        && data_slice
            .map(|s| s.length.min(account.data.len().saturating_sub(s.offset)))
            .unwrap_or(account.data.len())
            > MAX_BASE58_BYTES
    {
        let message = format!(
            "Encoded binary (base 58) data should be less than {MAX_BASE58_BYTES} bytes, please \
             use Base64 encoding."
        );
        Err(ErrorObject::owned(
            ErrorCode::InvalidRequest.code(),
            message,
            None::<()>,
        ))
    } else {
        Ok(encode_ui_account(
            pubkey, account, encoding, None, data_slice,
        ))
    }
}
