mod dbx;
use dbx::*;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod wasm;

use crate::param::DbResponse;
use crate::param::Param;
use crate::ErrorKind;
use crate::Method;
use crate::Result;
use crate::Route;
// #[cfg(not(target_arch = "wasm32"))]
// use futures::TryStreamExt;
use indexmap::IndexMap;

use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::mem;
use surrealdb::sql::statements::CreateStatement;
use surrealdb::sql::statements::DeleteStatement;
use surrealdb::sql::statements::SelectStatement;
use surrealdb::sql::statements::UpdateStatement;
use surrealdb::sql::Array;
use surrealdb::sql::Data;
use surrealdb::sql::Field;
use surrealdb::sql::Fields;
use surrealdb::sql::Output;
use surrealdb::sql::Strand;
use surrealdb::sql::Value;
use surrealdb::sql::Values;

use self::dbx::DbX;

type HttpRoute = Route<(Method, Param), Result<DbResponse>>;

/// An HTTP client for communicating with the server via HTTP
#[derive(Debug, Clone)]
pub struct Client {
    method: Method,
}

type RequestBuilder = String;

#[derive(Debug, Serialize, Deserialize)]
struct Root {
    user: String,
    pass: String,
}

async fn query(request: RequestBuilder) -> Result<Vec<Result<Vec<Value>>>> {
    tracing::info!("{request:?}");
    let responses = request.send().await?;

    tracing::info!("Response {responses:?}");
    into_value(responses)
}

fn into_value(responses: Vec<surrealdb::Response>) -> Result<Vec<Result<Vec<Value>>>> {
    let mut vec = Vec::with_capacity(responses.len());
    for response in responses {
        match response.result {
            Ok(value) => match value {
                Value::Array(Array(array)) => vec.push(Ok(array)),
                Value::None | Value::Null => vec.push(Ok(vec![])),
                value => vec.push(Ok(vec![value])),
            },
            Err(error) => {
                vec.push(Err(ErrorKind::Query.with_context(error)));
            }
        }
    }
    Ok(vec)
}

async fn take(one: bool, request: RequestBuilder) -> Result<Value> {
    if let Some(result) = query(request).await?.pop() {
        let mut vec = result?;
        match vec.pop() {
            Some(Value::Array(Array(mut vec))) => {
                if one {
                    if let [value] = &mut vec[..] {
                        return Ok(mem::take(value));
                    }
                } else {
                    return Ok(Value::Array(Array(vec)));
                }
            }
            Some(Value::None | Value::Null) | None => {}
            Some(value) => {
                return Ok(value);
            }
        }
    }
    match one {
        true => Ok(Value::None),
        false => Ok(Value::Array(Array(vec![]))),
    }
}

// #[cfg(not(target_arch = "wasm32"))]
// async fn export(request: RequestBuilder, file: PathBuf) -> Result<Value> {
//     let mut file = OpenOptions::new()
//         .write(true)
//         .create(true)
//         .truncate(true)
//         .open(file)
//         .await?;
//     let mut response = request
//         .send()
//         .await?
//         .error_for_status()?
//         .bytes_stream()
//         .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
//         .into_async_read()
//         .compat();
//     io::copy(&mut response, &mut file).await?;
//     Ok(Value::None)
// }

// #[cfg(not(target_arch = "wasm32"))]
// async fn import(request: RequestBuilder, file: PathBuf) -> Result<Value> {
//     let mut file = OpenOptions::new().read(true).open(file).await?;
//     let mut contents = vec![];
//     file.read_to_end(&mut contents).await?;
//     // ideally we should pass `file` directly into the body
//     // but currently that results in
//     // "HTTP status client error (405 Method Not Allowed) for url"
//     request.body(contents).send().await?.error_for_status()?;
//     Ok(Value::None)
// }

fn version() -> Result<Value> {
    use once_cell::sync::Lazy;

    pub const PKG_NAME: &str = "surrealdb";
    pub static PKG_VERSION: Lazy<String> =
        Lazy::new(|| match option_env!("SURREAL_BUILD_METADATA") {
            Some(metadata) if !metadata.trim().is_empty() => {
                let version = env!("CARGO_PKG_VERSION");
                format!("{version}+{metadata}")
            }
            _ => env!("CARGO_PKG_VERSION").to_owned(),
        });
    let val = format!("{}-{}", PKG_NAME, *PKG_VERSION);
    Ok(val.into())
}

fn split_params(params: &mut [Value]) -> (bool, Values, Value) {
    let (what, data) = match params {
        [what] => (mem::take(what), Value::None),
        [what, data] => (mem::take(what), mem::take(data)),
        _ => unreachable!(),
    };
    let one = what.is_thing();
    let what = match what {
        Value::Array(Array(vec)) => Values(vec),
        value => Values(vec![value]),
    };
    (one, what, data)
}

fn create_statement(params: &mut [Value]) -> CreateStatement {
    let (_, what, data) = split_params(params);
    let data = match data {
        Value::None => None,
        value => Some(Data::ContentExpression(value)),
    };
    CreateStatement {
        what,
        data,
        output: Some(Output::After),
        ..Default::default()
    }
}

fn update_statement(params: &mut [Value]) -> (bool, UpdateStatement) {
    let (one, what, data) = split_params(params);
    let data = match data {
        Value::None => None,
        value => Some(Data::ContentExpression(value)),
    };
    (
        one,
        UpdateStatement {
            what,
            data,
            output: Some(Output::After),
            ..Default::default()
        },
    )
}

fn patch_statement(params: &mut [Value]) -> (bool, UpdateStatement) {
    let (one, what, data) = split_params(params);
    let data = match data {
        Value::None => None,
        value => Some(Data::PatchExpression(value)),
    };
    (
        one,
        UpdateStatement {
            what,
            data,
            output: Some(Output::Diff),
            ..Default::default()
        },
    )
}

fn merge_statement(params: &mut [Value]) -> (bool, UpdateStatement) {
    let (one, what, data) = split_params(params);
    let data = match data {
        Value::None => None,
        value => Some(Data::MergeExpression(value)),
    };
    (
        one,
        UpdateStatement {
            what,
            data,
            output: Some(Output::After),
            ..Default::default()
        },
    )
}

fn select_statement(params: &mut [Value]) -> (bool, SelectStatement) {
    let (one, what, _) = split_params(params);
    (
        one,
        SelectStatement {
            what,
            expr: Fields(vec![Field::All]),
            ..Default::default()
        },
    )
}

fn delete_statement(params: &mut [Value]) -> DeleteStatement {
    let (_, what, _) = split_params(params);
    DeleteStatement {
        what,
        output: Some(Output::None),
        ..Default::default()
    }
}

async fn router(
    (method, param): (Method, Param),
    vars: &mut IndexMap<String, String>,
) -> Result<DbResponse> {
    let mut params = param.query;

    match method {
        Method::Use => {
            //
            let (ns, db) = match &mut params[..] {
                [Value::Strand(Strand(ns)), Value::Strand(Strand(db))] => {
                    (mem::take(ns), mem::take(db))
                }
                _ => unreachable!(),
            };
            #[cfg(feature = "test")]
            DbX::new(db, ns, "memory".to_owned());
            // todo!();

            #[cfg(not(feature = "test"))]
            DbX::new(&db, &ns, "memory")
                .await
                .unwrap_or_else(|_| panic!("use db error: {db} {ns}"));
            Ok(DbResponse::Other(Value::None))
        }

        Method::Create => {
            let statement = create_statement(&mut params);
            let request = statement.to_string();
            let value = take(true, request).await?;
            Ok(DbResponse::Other(value))
        }
        Method::Update => {
            let (one, statement) = update_statement(&mut params);
            let request = statement.to_string();
            let value = take(one, request).await?;
            Ok(DbResponse::Other(value))
        }
        Method::Patch => {
            let (one, statement) = patch_statement(&mut params);
            let request = statement.to_string();
            let value = take(one, request).await?;
            Ok(DbResponse::Other(value))
        }
        Method::Merge => {
            let (one, statement) = merge_statement(&mut params);
            let request = statement.to_string();
            let value = take(one, request).await?;
            Ok(DbResponse::Other(value))
        }
        Method::Select => {
            let (one, statement) = select_statement(&mut params);
            let request = statement.to_string();
            let value = take(one, request).await?;
            Ok(DbResponse::Other(value))
        }
        Method::Delete => {
            let statement = delete_statement(&mut params);
            let request = statement.to_string();
            let value = take(true, request).await?;
            Ok(DbResponse::Other(value))
        }
        Method::Query => {
            let request;
            let mut qvars: BTreeMap<String, Value> = vars
                .iter()
                .map(|(k, v)| (k.clone(), Value::from(v.clone())))
                .collect();
            match &mut params[..] {
                [Value::Strand(Strand(statements))] => {
                    request = statements;
                }
                [Value::Strand(Strand(statements)), Value::Object(bindings)] => {
                    request = statements;
                    qvars.extend(bindings.0.clone());
                }
                _ => unreachable!(),
            }
            let db = DBX.get().unwrap();

            // db.datastore.execute(request, db.sess, vars, false);
            let values = db
                .datastore
                .execute(request, &db.session, Some(qvars), false)
                .await?;
            let values = into_value(values)?;
            Ok(DbResponse::Query(values))
        }
        // #[cfg(not(target_arch = "wasm32"))]
        // Method::Export => {
        //     let path = base_url.join(Method::Export.as_str()).unwrap();
        //     let file = param.file.expect("file to export into");
        //     let request = client
        //         .get(path)
        //         .headers(headers.clone())
        //         .auth(&auth)
        //         .header(ACCEPT, "application/octet-stream");
        //     let value = export(request, file).await?;
        //     Ok(DbResponse::Other(value))
        // }
        // #[cfg(not(target_arch = "wasm32"))]
        // Method::Import => {
        //     let path = base_url.join(Method::Import.as_str()).unwrap();
        //     let file = param.file.expect("file to import from");
        //     let request = client
        //         .post(path)
        //         .headers(headers.clone())
        //         .auth(&auth)
        //         .header(CONTENT_TYPE, "application/octet-stream");
        //     let value = import(request, file).await?;
        //     Ok(DbResponse::Other(value))
        // }
        Method::Version => {
            let value = version()?;
            Ok(DbResponse::Other(value))
        }
        Method::Set => {
            let (key, value) = match &mut params[..2] {
                [Value::Strand(Strand(key)), value] => (mem::take(key), value.to_string()),
                _ => unreachable!(),
            };
            let request = format!("RETURN ${key}");
            take(true, request).await?;
            vars.insert(key, value);
            Ok(DbResponse::Other(Value::None))
        }
        Method::Unset => {
            if let [Value::Strand(Strand(key))] = &params[..1] {
                vars.remove(key);
            }
            Ok(DbResponse::Other(Value::None))
        }
        Method::Live => {
            let table = match &params[..] {
                [table] => table.to_string(),
                _ => unreachable!(),
            };
            let request = format!("LIVE SELECT * FROM type::table({table})");
            let value = take(true, request).await?;
            Ok(DbResponse::Other(value))
        }
        Method::Kill => {
            let id = match &params[..] {
                [id] => id.to_string(),
                _ => unreachable!(),
            };
            let request = format!("KILL type::string({id})");
            let value = take(true, request).await?;
            Ok(DbResponse::Other(value))
        }
        Method::Authenticate => todo!(),
        Method::Health => todo!(),
        Method::Invalidate => todo!(),
        Method::Signin => todo!(),
        Method::Signup => todo!(),
        Method::Export => todo!(),
        Method::Import => todo!(),
    }
}
