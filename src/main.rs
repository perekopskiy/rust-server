mod db_operations;
 
use actix_web::{web, App, HttpResponse, HttpServer};
use serde::{Deserialize, Serialize};
use std::{thread, time};

#[derive(Debug, Serialize, Deserialize)]
struct Request {
    task: Vec<u8>
}

enum CalculationResult {
    Success(Vec<u8>),
    Failure(String),
}

static mut POOL: Option<sqlx::postgres::PgPool> = None;

async fn post_request(item: web::Json<Request>) -> HttpResponse {
    unsafe {
        match &POOL {
            Some(pool) => {
                let conn = db_operations::retrive_connection(pool).await;
                match conn {
                    Ok(mut conn) => {
                        let id = db_operations::insert(&mut conn, &item.0.task).await;
                        match id {
                            Ok(id) => HttpResponse::Ok().json(id),
                            Err(_) => HttpResponse::BadRequest().finish() 
                        }
                    },
                    Err(_) => HttpResponse::BadRequest().finish()
                }
            }
            None => {
                HttpResponse::BadRequest().finish()
            }
        }
    }
}

async fn request_status(item: web::Json<i32>) -> HttpResponse {
    unsafe {
        match &POOL {
            Some(pool) => {
                let conn = db_operations::retrive_connection(pool).await;
                match conn {
                    Ok(mut conn) => {
                        let status = db_operations::get_status(&mut conn, item.0).await;
                        match status {
                            Ok(status) => {
                                if status == "completed" {
                                    let result = db_operations::get_result(&mut conn, item.0).await;
                                    return match result {
                                        Ok(result) => HttpResponse::Ok().json(result),
                                        Err(_) => HttpResponse::BadRequest().finish()
                                    };
                                }
                                else if status == "failed" {
                                    let error = db_operations::get_error(&mut conn, item.0).await;
                                    return match error {
                                        Ok(error) => HttpResponse::Ok().json(error),
                                        Err(_) => HttpResponse::BadRequest().finish()
                                    };
                                }
                                else {
                                    return HttpResponse::Ok().json(status);
                                }
                            },
                            Err(_) => HttpResponse::BadRequest().finish()
                        }
                    },
                    Err(_) => HttpResponse::BadRequest().finish()
                }
            }
            None => {
                HttpResponse::BadRequest().finish()
            }
        }
    }
}

async fn post_result(item: web::Json<i32>) -> HttpResponse {
    unsafe {
        match &POOL {
            Some(pool) => {
                let conn = db_operations::retrive_connection(pool).await;
                match conn {
                    Ok(mut conn) => {
                        let status = db_operations::get_status(&mut conn, item.0).await;
                        match status {
                            Ok(status) => {
                                if status == "completed" {
                                    let result = db_operations::get_result(&mut conn, item.0).await;
                                    return match result {
                                        Ok(result) => HttpResponse::Ok().json(result),
                                        Err(_) => HttpResponse::BadRequest().finish()
                                    };
                                }
                                else if status == "failed" {
                                    let error = db_operations::get_error(&mut conn, item.0).await;
                                    return match error {
                                        Ok(error) => HttpResponse::Ok().json(error),
                                        Err(_) => HttpResponse::BadRequest().finish()
                                    };
                                }
                                else {
                                    return HttpResponse::BadRequest().finish();
                                }
                            },
                            Err(_) => HttpResponse::BadRequest().finish()
                        }
                    },
                    Err(_) => HttpResponse::BadRequest().finish()
                }
            }
            None => {
                HttpResponse::BadRequest().finish()
            }
        }
    }
}

async fn calculate(task: Vec<u8>) -> CalculationResult {
    thread::sleep(time::Duration::from_millis(5000));
    if task.is_empty() {
        return CalculationResult::Failure(String::from("empty vector"));
    }
    else {
        return CalculationResult::Success(task.clone()); 
    }
}

async fn get_job() -> HttpResponse {
    unsafe {
        match &POOL {
            Some(pool) => {
                let conn = db_operations::retrive_connection(pool).await;
                match conn {
                    Ok(mut conn) => {
                        let rec = db_operations::get_pending_rec(&mut conn).await;
                        match rec {
                            Ok(rec) => {
                                db_operations::set_status(&mut conn, rec.id, "assigned").await.expect("error");
                                let result = calculate(rec.task).await;
                                match result {
                                    CalculationResult::Success(result) => {
                                        db_operations::set_status(&mut conn, rec.id, "completed").await.expect("error");
                                        db_operations::set_result(&mut conn, rec.id, &result).await.expect("error");
                                    },
                                    CalculationResult::Failure(result) => {
                                        db_operations::set_status(&mut conn, rec.id, "failed").await.expect("error");
                                        db_operations::set_error(&mut conn, rec.id, &result).await.expect("error");
                                    }
                                };
                                HttpResponse::Ok().finish()
                            },
                            Err(_) => HttpResponse::BadRequest().finish() 
                        }
                    },
                    Err(_) => HttpResponse::BadRequest().finish()
                }
            }
            None => {
                HttpResponse::BadRequest().finish()
            }
        }
    }
}



#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    
    unsafe{
        POOL = Some(db_operations::create_connection().await?);
    }
    
    
    
    HttpServer::new(|| {
        App::new()
            .service(web::resource("/post_request").route(web::post().to(post_request)))
            .service(web::resource("/request_status").route(web::post().to(request_status)))
            .service(web::resource("/get_job").route(web::get().to(get_job)))
            .service(web::resource("/post_result").route(web::post().to(post_result)))
    })
    .bind("127.0.0.1:9000")?
    .run()
    .await?;
    
    
    
    Ok(())
}