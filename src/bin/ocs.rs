use RTSassignment::ocs;

#[tokio::main]
async fn main() {
    ocs::runtime::run().await;
}
