use {
    crate::config::ConfigRpc,
    bytes::Bytes,
    http_body_util::{BodyExt, Empty as BodyEmpty},
    hyper::{Request, Response, StatusCode, body::Incoming as BodyIncoming, service::service_fn},
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    tokio::{net::TcpListener, task::JoinHandle},
    tokio_util::sync::CancellationToken,
    tracing::{debug, error, info},
};

pub async fn spawn(
    config: ConfigRpc,
    shutdown: CancellationToken,
) -> anyhow::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(config.endpoint).await?;
    info!("start server at: {}", config.endpoint);

    let jh = tokio::spawn(async move {
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
                move |req: Request<BodyIncoming>| {
                    async move {
                        // JSON-RPC
                        if req.uri().path() == "/" {
                            todo!()
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
    });

    Ok(jh)
}
