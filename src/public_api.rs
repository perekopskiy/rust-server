use crate::db_operations;
use crate::MyPgPool;
use actix_web::{web, HttpResponse};

pub async fn post_request(data: web::Data<MyPgPool>, item: web::Json<db_operations::Request>) -> HttpResponse {
    let conn = db_operations::retrive_connection(&data.pool).await;
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

pub async fn request_status(data: web::Data<MyPgPool>, item: web::Json<i32>) -> HttpResponse {
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
                        return HttpResponse::Ok().json(status);
                    }
                },
                Err(_) => HttpResponse::BadRequest().finish()
            }
        },
        Err(_) => HttpResponse::BadRequest().finish()
    }
}