use RTSassignment::gcs;

#[tokio::main]
async fn main() {
    gcs::runtime::run().await;
}
