use payments_engine_rs::run;

#[tokio::main]
async fn main() -> anyhow::Result<()>  {
    Ok(run().await?)
}
