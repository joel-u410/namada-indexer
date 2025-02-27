use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum_extra::extract::Query;
use axum_macros::debug_handler;
use bigdecimal::BigDecimal;

use crate::dto::ibc::{
    IbcRateLimit as IbcRateLimitDto, IbcTokenFlow as IbcTokenFlowDto,
};
use crate::error::api::ApiError;
use crate::response::ibc::{
    IbcAck, IbcRateLimit, IbcTokenFlow, IbcTokenThroughput,
};
use crate::state::common::CommonState;

#[debug_handler]
pub async fn get_ibc_status(
    _headers: HeaderMap,
    Path(tx_id): Path<String>,
    State(state): State<CommonState>,
) -> Result<Json<IbcAck>, ApiError> {
    let ibc_ack_status = state.ibc_service.get_ack_by_tx_id(tx_id).await?;

    Ok(Json(ibc_ack_status))
}

#[debug_handler]
pub async fn get_ibc_rate_limits(
    Query(query): Query<IbcRateLimitDto>,
    State(state): State<CommonState>,
) -> Result<Json<Vec<IbcRateLimit>>, ApiError> {
    let rate_limits = state
        .ibc_service
        .get_throughput_limits(
            query.token_address,
            query.throughput_limit.map(BigDecimal::from),
        )
        .await?;

    Ok(Json(rate_limits))
}

#[debug_handler]
pub async fn get_ibc_token_flows(
    Query(query): Query<IbcTokenFlowDto>,
    State(state): State<CommonState>,
) -> Result<Json<Vec<IbcTokenFlow>>, ApiError> {
    let token_flows = state
        .ibc_service
        .get_token_flows(query.token_address)
        .await?;

    Ok(Json(token_flows))
}

#[debug_handler]
pub async fn get_ibc_token_throughput(
    //_headers: HeaderMap,
    Path(token): Path<String>,
    State(state): State<CommonState>,
) -> Result<Json<IbcTokenThroughput>, ApiError> {
    let throughput = state.ibc_service.get_token_throughput(token).await?;

    Ok(Json(throughput))
}
