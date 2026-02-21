use {
    crate::{config::ConfigRpc, storage::read::ReadRequest},
    futures::future::BoxFuture,
    jsonrpsee_types::{
        Extensions, Id, Params, Request, Response, ResponsePayload, TwoPointZero,
        error::INVALID_PARAMS_MSG,
    },
    richat_shared::jsonrpc::{
        helpers::{
            jsonrpc_error_invalid_params, jsonrpc_response_error, jsonrpc_response_error_custom,
            jsonrpc_response_success, to_vec,
        },
        requests::{RpcRequestResult, RpcRequestsProcessor},
    },
    serde::{Deserialize, de},
    solana_commitment_config::CommitmentLevel,
    solana_rpc_client::api::{
        config::RpcContextConfig, custom_error::RpcCustomError, response::RpcVersionInfo,
    },
    std::{
        sync::{Arc, mpsc},
        time::Duration,
    },
};

#[derive(Debug)]
pub struct State {
    request_timeout: Duration,
    req_tx: mpsc::SyncSender<ReadRequest>,
}

pub fn create_request_processor(
    config: ConfigRpc,
    req_tx: mpsc::SyncSender<ReadRequest>,
) -> RpcRequestsProcessor<Arc<State>> {
    let mut processor = RpcRequestsProcessor::new(
        config.body_limit,
        Arc::new(State {
            request_timeout: config.request_timeout,
            req_tx,
        }),
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
            match Self::parse(state, x_subscription_id, upstream_disabled, request) {
                Ok(req) => req.process().await,
                Err(response) => Ok(response),
            }
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
struct RpcRequestSlot;

impl RpcRequestHandler for RpcRequestSlot {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
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
        let commitment = commitment.unwrap_or_default();

        // let context_slot = match commitment.commitment {
        //     CommitmentLevel::Processed => state.stored_slots.processed.load(Ordering::Relaxed),
        //     CommitmentLevel::Confirmed => state.stored_slots.confirmed.load(Ordering::Relaxed),
        //     CommitmentLevel::Finalized => state.stored_slots.finalized.load(Ordering::Relaxed),
        // };
        let context_slot = 0; // TODO

        if let Some(min_context_slot) = min_context_slot
            && context_slot < min_context_slot
        {
            return Err(jsonrpc_response_error_custom(
                id,
                RpcCustomError::MinContextSlotNotReached { context_slot },
            ));
        }

        Err(jsonrpc_response_success(id, context_slot))
    }
}

#[derive(Debug)]
struct RpcRequestAccountInfo;

impl RpcRequestHandler for RpcRequestAccountInfo {
    fn parse(
        _state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        _request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct RpcRequestMultipleAccounts;

impl RpcRequestHandler for RpcRequestMultipleAccounts {
    fn parse(
        _state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        _request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        unimplemented!()
    }
}
