use {
    crate::{grpc::GeyserMessage, rocksdb::Rocksdb},
    tokio::sync::mpsc,
    tokio_util::sync::CancellationToken,
};

pub async fn start(
    db: Rocksdb,
    update_rx: mpsc::Receiver<GeyserMessage>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    todo!()
}
