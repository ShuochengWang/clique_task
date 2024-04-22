use super::*;

use dotenv::dotenv;

use std::env;

async fn start_server() {
    dotenv().ok();

    let uri = env::var("DATABASE_URI").expect("DATABASE_URI must be set");
    let user = env::var("DATABASE_USERNAME").expect("DATABASE_USERNAME must be set");
    let pass = env::var("DATABASE_PASSWORD").expect("DATABASE_PASSWORD must be set");

    let graph = neo4rs::Graph::new(uri, user, pass).await.unwrap();
}
