//! Shows the production migration entry point for PostgreSQL deployments.
use actryn::PostgresStorage;
use deadpool_postgres::{Manager, Pool};
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("ACTOR_BENCH_DATABASE_URL").expect(
        "ACTOR_BENCH_DATABASE_URL must point to a dedicated disposable PostgreSQL database",
    );
    let config = url.parse::<tokio_postgres::Config>()?;
    let pool = Pool::builder(Manager::new(config, NoTls))
        .max_size(4)
        .build()?;
    let storage = PostgresStorage::new(pool);

    storage.migrate().await?;
    let version = storage.schema_version().await?;
    storage.migrate().await?;
    assert_eq!(version, storage.schema_version().await?);
    println!("schema_version={version}");
    Ok(())
}
