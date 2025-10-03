use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum_extra::extract::Query;
use axum_macros::debug_handler;

use crate::dto::transaction::{
    TransactionHistoryQueryParams, TransactionIdParam,
    TransactionMostRecentQueryParams,
};
use crate::error::api::ApiError;
use crate::error::transaction::TransactionError;
use crate::response::headers;
use crate::response::transaction::{
    InnerTransactionResponse, TransactionHistoryResponse,
    WrapperTransactionResponse,
};
use crate::response::utils::PaginatedResponse;
use crate::state::common::CommonState;

#[debug_handler]
pub async fn get_wrapper_tx(
    _headers: HeaderMap,
    Path(tx_id): Path<TransactionIdParam>,
    State(state): State<CommonState>,
) -> Result<(HeaderMap, Json<Option<WrapperTransactionResponse>>), ApiError> {
    tx_id.is_valid_hash()?;

    let tx_id = tx_id.get();

    let wrapper_tx = state
        .transaction_service
        .get_wrapper_tx(tx_id.clone())
        .await?;

    if wrapper_tx.is_none() {
        return Err(TransactionError::TxIdNotFound(tx_id).into());
    }

    let inner_txs = state
        .transaction_service
        .get_inner_tx_by_wrapper_id(tx_id)
        .await?;

    let response = wrapper_tx
        .map(|wrapper| WrapperTransactionResponse::new(wrapper, inner_txs));
    let headers = headers::with_cache();

    Ok((headers, Json(response)))
}

#[debug_handler]
pub async fn get_inner_tx(
    _headers: HeaderMap,
    Path(tx_id): Path<TransactionIdParam>,
    State(state): State<CommonState>,
) -> Result<(HeaderMap, Json<Option<InnerTransactionResponse>>), ApiError> {
    tx_id.is_valid_hash()?;

    let tx_id = tx_id.get();

    let inner_tx = state
        .transaction_service
        .get_inner_tx(tx_id.clone())
        .await?;

    let response = inner_tx.map(InnerTransactionResponse::new);
    let headers = headers::with_cache();

    Ok((headers, Json(response)))
}

#[debug_handler]
pub async fn get_transaction_history(
    _headers: HeaderMap,
    Query(query): Query<TransactionHistoryQueryParams>,
    State(state): State<CommonState>,
) -> Result<Json<PaginatedResponse<Vec<TransactionHistoryResponse>>>, ApiError>
{
    let page = query.page.unwrap_or(1);

    let (transactions, total_pages, total_items) = state
        .transaction_service
        .get_addresses_history(query.addresses, page)
        .await?;

    let response = transactions
        .into_iter()
        .map(TransactionHistoryResponse::from)
        .collect();

    Ok(Json(PaginatedResponse::new(
        response,
        page,
        total_pages,
        total_items,
    )))
}

#[debug_handler]
pub async fn get_most_recent_transactions(
    _headers: HeaderMap,
    Query(query): Query<TransactionMostRecentQueryParams>,
    State(state): State<CommonState>,
) -> Result<Json<Vec<WrapperTransactionResponse>>, ApiError> {
    let size = query.size.unwrap_or(10);

    let transactions = state
        .transaction_service
        .get_most_recent_transactions(size)
        .await?;

    let inner_txs = transactions
        .iter()
        .map(|tx| {
            state
                .transaction_service
                .get_inner_tx_by_wrapper_id(tx.id.to_string())
        })
        .collect::<Vec<_>>();

    let inner_txs = futures::future::join_all(inner_txs).await;

    let response = transactions
        .into_iter()
        .zip(inner_txs.into_iter())
        .map(|(tx, inner_tx_result)| {
            let inner_txs = inner_tx_result.unwrap_or_default();
            WrapperTransactionResponse::new(tx, inner_txs)
        })
        .collect();

    Ok(Json(response))
}
