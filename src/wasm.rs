#![allow(missing_docs)]
use crate::SimpleAsyncConnection;
use diesel::backend::Backend;
use diesel::connection::Instrumentation;
use diesel::connection::{
    InTransactionStatus, TransactionDepthChange, TransactionManagerStatus,
    ValidTransactionManagerStatus,
};
use diesel::deserialize::FromSqlRow;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::result::Error;
use diesel::row::Row;
use diesel::{ConnectionResult, QueryResult};
use futures_util::{stream, Future, Stream, StreamExt};
use scoped_futures::{ScopedFutureExt, ScopedLocalBoxFuture};
use std::fmt::Debug;

#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait(?Send)]
pub trait AsyncConnection: SimpleAsyncConnection + Sized {
    type ExecuteFuture<'conn, 'query>: Future<Output = QueryResult<usize>>;
    type LoadFuture<'conn, 'query>: Future<Output = QueryResult<Self::Stream<'conn, 'query>>>;
    type Stream<'conn, 'query>: Stream<Item = QueryResult<Self::Row<'conn, 'query>>>;
    type Row<'conn, 'query>: Row<'conn, Self::Backend>;
    type Backend: Backend;
    #[doc(hidden)]
    type TransactionManager: TransactionManager<Self>;

    async fn establish(database_url: &str) -> ConnectionResult<Self>;

    async fn transaction<'a, R, E, F>(&mut self, callback: F) -> Result<R, E>
    where
        F: for<'r> FnOnce(&'r mut Self) -> ScopedLocalBoxFuture<'a, 'r, Result<R, E>> + 'a,
        E: From<diesel::result::Error> + 'a,
        R: 'a,
    {
        Self::TransactionManager::transaction(self, callback).await
    }

    async fn begin_test_transaction(&mut self) -> QueryResult<()> {
        use diesel::connection::TransactionManagerStatus;

        match Self::TransactionManager::transaction_manager_status_mut(self) {
            TransactionManagerStatus::Valid(valid_status) => {
                assert_eq!(None, valid_status.transaction_depth())
            }
            TransactionManagerStatus::InError => panic!("Transaction manager in error"),
        };
        Self::TransactionManager::begin_transaction(self).await?;
        // set the test transaction flag
        // to prevent that this connection gets dropped in connection pools
        // Tests commonly set the poolsize to 1 and use `begin_test_transaction`
        // to prevent modifications to the schema
        Self::TransactionManager::transaction_manager_status_mut(self).set_test_transaction_flag();
        Ok(())
    }

    async fn test_transaction<'a, R, E, F>(&'a mut self, f: F) -> R
    where
        F: for<'r> FnOnce(&'r mut Self) -> ScopedLocalBoxFuture<'a, 'r, Result<R, E>> + 'a,
        E: Debug + 'a,
        R: 'a,
        Self: 'a,
    {
        use futures_util::TryFutureExt;

        let mut user_result = None;
        let _ = self
            .transaction::<R, _, _>(|c| {
                f(c).map_err(|_| Error::RollbackTransaction)
                    .and_then(|r| {
                        user_result = Some(r);
                        futures_util::future::ready(Err(Error::RollbackTransaction))
                    })
                    .scope_boxed_local()
            })
            .await;
        user_result.expect("Transaction did not succeed")
    }

    #[doc(hidden)]
    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + 'query;

    #[doc(hidden)]
    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Self::Backend> + QueryId + 'query;

    #[doc(hidden)]
    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData;

    #[doc(hidden)]
    fn _silence_lint_on_execute_future(_: Self::ExecuteFuture<'_, '_>) {}
    #[doc(hidden)]
    fn _silence_lint_on_load_future(_: Self::LoadFuture<'_, '_>) {}

    #[doc(hidden)]
    fn instrumentation(&mut self) -> &mut dyn Instrumentation;

    /// Set a specific [`Instrumentation`] implementation for this connection
    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation);
}

#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait(?Send)]
pub trait TransactionManager<Conn: AsyncConnection> {
    type TransactionStateData;

    async fn begin_transaction(conn: &mut Conn) -> QueryResult<()>;

    async fn rollback_transaction(conn: &mut Conn) -> QueryResult<()>;

    async fn commit_transaction(conn: &mut Conn) -> QueryResult<()>;

    #[doc(hidden)]
    fn transaction_manager_status_mut(conn: &mut Conn) -> &mut TransactionManagerStatus;

    async fn transaction<'a, F, R, E>(conn: &mut Conn, callback: F) -> Result<R, E>
    where
        F: for<'r> FnOnce(&'r mut Conn) -> ScopedLocalBoxFuture<'a, 'r, Result<R, E>> + 'a,
        E: From<Error>,
    {
        Self::begin_transaction(conn).await?;
        match callback(&mut *conn).await {
            Ok(value) => {
                Self::commit_transaction(conn).await?;
                Ok(value)
            }
            Err(user_error) => match Self::rollback_transaction(conn).await {
                Ok(()) => Err(user_error),
                Err(Error::BrokenTransactionManager) => {
                    // In this case we are probably more interested by the
                    // original error, which likely caused this
                    Err(user_error)
                }
                Err(rollback_error) => Err(rollback_error.into()),
            },
        }
    }

    #[doc(hidden)]
    fn is_broken_transaction_manager(conn: &mut Conn) -> bool {
        match Self::transaction_manager_status_mut(conn).transaction_state() {
            // all transactions are closed
            // so we don't consider this connection broken
            Ok(ValidTransactionManagerStatus {
                in_transaction: None,
                ..
            }) => false,
            // The transaction manager is in an error state
            // Therefore we consider this connection broken
            Err(_) => true,
            // The transaction manager contains a open transaction
            // we do consider this connection broken
            // if that transaction was not opened by `begin_test_transaction`
            Ok(ValidTransactionManagerStatus {
                in_transaction: Some(s),
                ..
            }) => !s.test_transaction,
        }
    }
}

pub trait LoadQuery<'query, Conn: AsyncConnection, U> {
    /// The future returned by [`LoadQuery::internal_load`]
    type LoadFuture<'conn>: Future<Output = QueryResult<Self::Stream<'conn>>>
    where
        Conn: 'conn;
    /// The inner stream returned by [`LoadQuery::internal_load`]
    type Stream<'conn>: Stream<Item = QueryResult<U>>
    where
        Conn: 'conn;

    /// Load this query
    fn internal_load(self, conn: &mut Conn) -> Self::LoadFuture<'_>;
}

#[allow(clippy::type_complexity)]
pub fn map_result_stream_future<'s, 'a, U, S, R, DB, ST>(
    stream: S,
) -> stream::Map<S, fn(QueryResult<R>) -> QueryResult<U>>
where
    S: Stream<Item = QueryResult<R>> + 's,
    R: diesel::row::Row<'a, DB> + 's,
    DB: Backend + 'static,
    U: FromSqlRow<ST, DB> + 'static,
    ST: 'static,
{
    stream.map(map_row_helper::<_, DB, U, ST>)
}

pub fn map_row_helper<'a, R, DB, U, ST>(row: QueryResult<R>) -> QueryResult<U>
where
    U: FromSqlRow<ST, DB>,
    R: diesel::row::Row<'a, DB>,
    DB: Backend,
{
    U::build_from_row(&row?).map_err(diesel::result::Error::DeserializationError)
}
