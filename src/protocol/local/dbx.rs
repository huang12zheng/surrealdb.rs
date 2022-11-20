// use crate::*;
use once_cell::sync::OnceCell;
use surrealdb::{Datastore, Error, Response, Session};
pub static DBX: OnceCell<DbX> = OnceCell::new();
// surrealdb

pub struct DbX {
    pub datastore: Datastore,
    pub session: Session,
}

impl DbX {
    pub async fn new(namespace: &str, database: &str, path: &str) -> Result<(), DbX> {
        let datastore = Datastore::new(path).await.unwrap();
        let session = Session::for_db(namespace, database);
        DBX.set(Self {
            datastore,
            session: session,
        })
    }

    pub async fn raw_execute(&self, txt: String) -> Result<Vec<Response>, Error> {
        self.datastore
            .execute(&txt, &self.session, None, false)
            .await
    }
}

pub mod strx {
    use async_trait::async_trait;
    use surrealdb::{Error, Response};

    use super::DBX;

    #[async_trait]
    pub trait StrDb {
        async fn send(&self) -> Result<Vec<Response>, Error>;
    }
    #[async_trait]
    impl StrDb for &str {
        async fn send(&self) -> Result<Vec<Response>, Error> {
            DBX.get().unwrap().raw_execute(self.to_string()).await
        }
    }

    #[async_trait]
    impl StrDb for String {
        async fn send(&self) -> Result<Vec<Response>, Error> {
            self.as_str().send().await
        }
    }
}
pub use strx::*;
