use anyhow::Context;
use chrono::NaiveDateTime;
use diesel::upsert::excluded;
use diesel::{ExpressionMethods, PgConnection, RunQueryDsl};
use orm::crawler_state::{BlockStateInsertDb, CrawlerNameDb};
use orm::schema::{crawler_state, inner_transactions, wrapper_transactions};
use orm::transactions::{InnerTransactionInsertDb, WrapperTransactionInsertDb};
use shared::crawler_state::{BlockCrawlerState, CrawlerName};
use shared::transaction::{InnerTransaction, WrapperTransaction};

pub fn insert_inner_transactions(
    transaction_conn: &mut PgConnection,
    txs: Vec<InnerTransaction>,
) -> anyhow::Result<()> {
    diesel::insert_into(inner_transactions::table)
        .values::<&Vec<InnerTransactionInsertDb>>(
            &txs.into_iter()
                .map(InnerTransactionInsertDb::from)
                .collect::<Vec<_>>(),
        )
        .on_conflict(inner_transactions::id)
        .do_update()
        .set((
            // Allow updating transactions kind + data so that if the indexer
            // is updated with new transaction type support, we can
            // easily go back & reindex any old transactions
            // that were previously marked as "unknown".
            inner_transactions::kind.eq(excluded(inner_transactions::kind)),
            inner_transactions::data.eq(excluded(inner_transactions::data)),
        ))
        .execute(transaction_conn)
        .context("Failed to insert inner transactions in db")?;

    anyhow::Ok(())
}

pub fn insert_wrapper_transactions(
    transaction_conn: &mut PgConnection,
    txs: Vec<WrapperTransaction>,
) -> anyhow::Result<()> {
    diesel::insert_into(wrapper_transactions::table)
        .values::<&Vec<WrapperTransactionInsertDb>>(
            &txs.into_iter()
                .map(WrapperTransactionInsertDb::from)
                .collect::<Vec<_>>(),
        )
        .on_conflict_do_nothing()
        .execute(transaction_conn)
        .context("Failed to insert wrapper transactions in db")?;

    anyhow::Ok(())
}

pub fn insert_crawler_state(
    transaction_conn: &mut PgConnection,
    crawler_state: BlockCrawlerState,
) -> anyhow::Result<()> {
    diesel::insert_into(crawler_state::table)
        .values::<&BlockStateInsertDb>(
            &(CrawlerName::Transactions, crawler_state).into(),
        )
        .on_conflict(crawler_state::name)
        .do_update()
        .set((
            crawler_state::timestamp.eq(excluded(crawler_state::timestamp)),
            crawler_state::last_processed_block
                .eq(excluded(crawler_state::last_processed_block)),
        ))
        .execute(transaction_conn)
        .context("Failed to update crawler state in db")?;

    anyhow::Ok(())
}

pub fn update_crawler_timestamp(
    transaction_conn: &mut PgConnection,
    timestamp: NaiveDateTime,
) -> anyhow::Result<()> {
    diesel::update(crawler_state::table)
        .filter(
            crawler_state::name
                .eq(CrawlerNameDb::from(CrawlerName::Transactions)),
        )
        .set(crawler_state::timestamp.eq(timestamp))
        .execute(transaction_conn)
        .context("Failed to update crawler timestamp in db")?;

    anyhow::Ok(())
}
