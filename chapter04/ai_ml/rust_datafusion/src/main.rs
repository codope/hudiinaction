// Chapter 4: query the store_sales Hudi table with Apache DataFusion through hudi-rs.
// Run from this directory: cargo run --release
use std::sync::Arc;
use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use hudi::HudiDataSource;
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let hudi = HudiDataSource::new_with_options(
        "/tmp/hudi_demo_tbl",
        [("hoodie.read.input.partitions", "5")]).await?;
    ctx.register_table("hudi_demo_tbl", Arc::new(hudi))?;
    let df: DataFrame = ctx.sql("SELECT ss_store_sk, count(*) FROM hudi_demo_tbl GROUP BY ss_store_sk ORDER BY ss_store_sk").await?;
    df.show().await?;
    Ok(())
}
