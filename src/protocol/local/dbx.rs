// use crate::*;
use once_cell::sync::OnceCell;
use surrealdb::{Datastore, Session};
pub static DBX: OnceCell<DbX> = OnceCell::new();

pub struct DbX {
    pub datastore: Datastore,
    pub session: Session,
}

impl DbX {
    pub async fn new(namespace: String, database: String, path: String) -> Result<(), DbX> {
        let datastore = Datastore::new(&path).await.unwrap();
        let session = Session::for_db(namespace.to_string(), database.to_string());
        DBX.set(Self {
            datastore,
            session: session,
        })
    }

    pub async fn raw_execute(&self, txt: String) -> Result<Vec<Value>, Error> {
        Ok(self
            .datastore
            .execute(&txt, &self.session, None, false)
            .await
            .unwrap())
    }
    // pub async fn raw_execute_one(&self, txt: String) -> Result<Vec<Value>, Error> {
    //     Ok(first(
    //         self.datastore
    //             .execute(&txt, &self.session, None, false)
    //             .await
    //             .unwrap(),
    //     ))
    // }
}

// fn first(responses: Vec<surrealdb::Response>) -> Vec<Value> {
//     responses
//         .into_iter()
//         .map(|response| response.result.unwrap())
//         .next()
//         .map(|result_value| Vec::<Value>::try_from(result_value).unwrap())
//         .unwrap()
// }
