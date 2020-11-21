mod db_operations;
mod public_api;
mod private_api; 

use sqlx::postgres::PgPool;
use actix_web::{web, App, HttpServer};

pub struct MyPgPool {
    pub pool: PgPool
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let pool = db_operations::create_connection().await?;
    
    HttpServer::new(move || {
        App::new()
            .data(MyPgPool{pool: pool.clone()})
            .service(web::resource("/post_request").route(web::post().to(public_api::post_request)))
            .service(web::resource("/request_status").route(web::post().to(public_api::request_status)))
            .service(web::resource("/get_job").route(web::get().to(private_api::get_job)))
            .service(web::resource("/post_result").route(web::post().to(private_api::post_result)))
    })
    .bind("127.0.0.1:9000")?
    .run()
    .await?;
    
    Ok(())
}