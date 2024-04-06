use payments_engine_rs::run;

#[tokio::main]
async fn main() -> anyhow::Result<()>  {
    // env_logger::init();
    Ok(run().await?)
}
