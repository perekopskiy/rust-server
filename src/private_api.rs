use crate::db_operations;
use crate::db_operations::CalculationResult;
use crate::MyPgPool;
use actix_web::{web, HttpResponse};
use std::{thread, time};

pub async fn post_result(data: web::Data<MyPgPool>, item: web::Json<i32>) -> HttpResponse {
    let conn = db_operations::retrive_connection(&data.pool).await;
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

async fn calculate(task: Vec<u8>) -> CalculationResult {
    thread::sleep(time::Duration::from_millis(5000));
    if task.is_empty() {
        return CalculationResult::Failure(String::from("empty vector"));
    }
    else {
        return CalculationResult::Success(task.clone()); 
    }
}

pub async fn get_job(data: web::Data<MyPgPool>) -> HttpResponse {
    let conn = db_operations::retrive_connection(&data.pool).await;
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