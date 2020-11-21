use super::db_operations;

#[actix_rt::test]
async fn test_db_operations() -> anyhow::Result<()> {
    let pool = db_operations::create_connection().await?;
    let mut conn = db_operations::retrive_connection(&pool).await?;
    
    let v : Vec<u8> = vec![1, 2, 3];
    let id = db_operations::insert(&mut conn, &v).await?;
    let status = db_operations::get_status(&mut conn, id).await?;
    assert_eq!(status, "pending");

    db_operations::get_pending_rec(&mut conn).await?; // should be at least one pending record because he have just insert one

    db_operations::set_status(&mut conn, id, "assigned").await?;
    let status = db_operations::get_status(&mut conn, id).await?;
    assert_eq!(status, "assigned");

    let res = db_operations::get_calculation_result(&mut conn, id).await;
    match res {
        Ok(_) => {panic!("found result but it shouldn`t exist yet");},
        Err(_) => {}
    };

    db_operations::set_status(&mut conn, id, "failed").await?;
    db_operations::set_calculation_result(&mut conn, id, &db_operations::CalculationResult::Failure(String::from("abc"))).await?;
    let res = db_operations::get_calculation_result(&mut conn, id).await;
    match res {
        Ok(result) => {
            match result {
                db_operations::CalculationResult::Failure(error) => {
                    assert_eq!(error, "abc");
                }
                db_operations::CalculationResult::Success(_) => {
                    panic!("found success but it should be failure");
                }
            };
        },
        Err(_) => {
            panic!("didn`t find result but it should exist");
        }
    };

    Ok(())
}