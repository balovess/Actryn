use actryn::SqliteStorage;

#[tokio::test]
async fn sqlite_obeys_the_storage_contract() {
    let directory = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(directory.path().join("contract.db")).unwrap();

    actryn_test::storage::verify(&storage).await.unwrap();
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_obeys_the_storage_contract() {
    use actryn::PostgresStorage;
    use deadpool_postgres::{Manager, Pool};
    use tokio_postgres::NoTls;

    let url = std::env::var("ACTOR_TEST_DATABASE_URL")
        .expect("ACTOR_TEST_DATABASE_URL must point to an isolated disposable database");
    let config = url.parse::<tokio_postgres::Config>().expect("database URL");
    let pool = Pool::builder(Manager::new(config, NoTls))
        .max_size(8)
        .build()
        .expect("connection pool");
    let storage = PostgresStorage::new(pool);
    storage.initialize_schema().await.unwrap();

    actryn_test::storage::verify(&storage).await.unwrap();
}
