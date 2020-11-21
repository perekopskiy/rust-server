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
                    if status != "completed" && status != "failed" {
                        return HttpResponse::Ok().json(status);
                    }
                    else {
                        let result = db_operations::get_calculation_result(&mut conn, item.0).await;
                        return match result {
                            Ok(result) => HttpResponse::Ok().json(result),
                            Err(_) => HttpResponse::BadRequest().finish()
                        };
                    }
                },
                Err(_) => HttpResponse::BadRequest().finish()
            }
        },
        Err(_) => HttpResponse::BadRequest().finish()
    }
}