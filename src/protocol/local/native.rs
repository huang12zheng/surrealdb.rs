use super::Client;
use super::HttpRoute;
use crate::param::from_value;
use crate::param::DbResponse;
use crate::param::Param;
use crate::param::ServerAddrs;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::param::Tls;
use crate::Connection;
use crate::Method;
use crate::Result;
use crate::Route;
use crate::Router;
use crate::Surreal;
use async_trait::async_trait;
use flume::Receiver;
use futures::StreamExt;
use indexmap::IndexMap;
use once_cell::sync::OnceCell;

use serde::de::DeserializeOwned;
use std::marker::PhantomData;
#[cfg(feature = "ws")]
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use surrealdb::sql::Value;
use url::Url;

#[async_trait]
impl Connection for Client {
    type Request = (Method, Param);
    type Response = Result<DbResponse>;

    fn new(method: Method) -> Self {
        Self { method }
    }

    async fn connect(address: ServerAddrs, capacity: usize) -> Result<Surreal<Self>> {
        let base_url = address.endpoint;

        let (route_tx, route_rx) = match capacity {
            0 => flume::unbounded(),
            capacity => flume::bounded(capacity),
        };

        router(base_url, route_rx);

        Ok(Surreal {
            router: OnceCell::with_value(Arc::new(Router {
                conn: PhantomData,
                sender: route_tx,
                #[cfg(feature = "ws")]
                last_id: AtomicI64::new(0),
            })),
        })
    }

    async fn send(
        &mut self,
        router: &Router<Self>,
        param: Param,
    ) -> Result<Receiver<Self::Response>> {
        let (sender, receiver) = flume::bounded(1);
        let route = Route {
            request: (self.method, param),
            response: sender,
        };
        router.sender.send_async(Some(route)).await?;
        Ok(receiver)
    }

    async fn recv<R>(&mut self, rx: Receiver<Self::Response>) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let response = rx.into_recv_async().await?;
        tracing::trace!("Response {response:?}");
        match response? {
            DbResponse::Other(value) => from_value(&value),
            DbResponse::Query(..) => unreachable!(),
        }
    }

    async fn recv_query(
        &mut self,
        rx: Receiver<Self::Response>,
    ) -> Result<Vec<Result<Vec<Value>>>> {
        let response = rx.into_recv_async().await?;
        tracing::trace!("Response {response:?}");
        match response? {
            DbResponse::Query(results) => Ok(results),
            DbResponse::Other(..) => unreachable!(),
        }
    }
}

fn router(base_url: Url, route_rx: Receiver<Option<HttpRoute>>) {
    tokio::spawn(async move {
        let mut vars = IndexMap::new();
        let mut stream = route_rx.into_stream();

        while let Some(Some(route)) = stream.next().await {
            match super::router(
                route.request,
                &base_url,
                // &client,
                // &mut headers,
                &mut vars,
            )
            .await
            {
                Ok(value) => {
                    let _ = route.response.into_send_async(Ok(value)).await;
                }
                Err(error) => {
                    let _ = route.response.into_send_async(Err(error)).await;
                }
            }
        }
    });
}
