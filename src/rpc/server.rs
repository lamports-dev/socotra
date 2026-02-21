use {
    crate::{
        config::ConfigRpc, rpc::jsonrpc::create_request_processor, storage::read::ReadRequest,
    },
    bytes::Bytes,
    http_body_util::{BodyExt, Empty as BodyEmpty},
    hyper::{Request, Response, StatusCode, body::Incoming as BodyIncoming, service::service_fn},
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    std::sync::{Arc, mpsc},
    tokio::net::TcpListener,
    tokio_util::sync::CancellationToken,
    tracing::{debug, error, info},
};

pub async fn spawn(
    config: ConfigRpc,
    req_tx: mpsc::SyncSender<ReadRequest>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(config.endpoint).await?;
    info!("start server at: {}", config.endpoint);

    let jsonrpc_processor = Arc::new(create_request_processor(config, req_tx));

    tokio::spawn(async move {
        let http = ServerBuilder::new(TokioExecutor::new());
        let graceful = hyper_util::server::graceful::GracefulShutdown::new();

        loop {
            let stream = tokio::select! {
                incoming = listener.accept() => match incoming {
                    Ok((stream, addr)) => {
                        debug!("new connection from {addr}");
                        stream
                    }
                    Err(error) => {
                        error!("failed to accept new connection: {error}");
                        break;
                    }
                },
                () = shutdown.cancelled() => break,
            };

            let service = service_fn({
                let jsonrpc_processor = Arc::clone(&jsonrpc_processor);
                move |req: Request<BodyIncoming>| {
                    let jsonrpc_processor = Arc::clone(&jsonrpc_processor);
                    async move {
                        // JSON-RPC
                        if req.uri().path() == "/" {
                            return jsonrpc_processor.on_request(req).await;
                        }

                        Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(BodyEmpty::<Bytes>::new().boxed())
                    }
                }
            });

            let connection = http.serve_connection(TokioIo::new(stream), service);
            let fut = graceful.watch(connection.into_owned());

            tokio::spawn(async move {
                if let Err(error) = fut.await {
                    error!("Error serving HTTP connection: {error:?}");
                }
            });
        }

        drop(listener);
        graceful.shutdown().await;
    })
    .await
    .map_err(Into::into)
}
