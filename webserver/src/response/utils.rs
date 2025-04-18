use namada_sdk::state::EPOCH_SWITCH_BLOCKS_DELAY;
use serde::Serialize;

use crate::constant::ITEM_PER_PAGE;

#[derive(Clone, Debug, Serialize)]
pub struct PaginatedResponse<T: Serialize> {
    pub results: T,
    pub pagination: Pagination,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Pagination {
    pub page: u64,
    pub per_page: u64,
    pub total_pages: u64,
    pub total_items: u64,
}

impl<T> PaginatedResponse<T>
where
    T: Serialize,
{
    pub fn new(
        results: T,
        page: u64,
        total_pages: u64,
        total_items: u64,
    ) -> Self {
        Self {
            results,
            pagination: Pagination {
                page,
                per_page: ITEM_PER_PAGE,
                total_pages,
                total_items,
            },
        }
    }
}

pub fn epoch_progress(
    current_block: i32,
    first_block_in_epoch: i32,
    blocks_per_epoch: i32,
) -> f64 {
    let blocks_per_epoch =
        blocks_per_epoch + (EPOCH_SWITCH_BLOCKS_DELAY as i32);

    // We remove 1 from the current_block so progress resets to 0 when new epoch
    // starts
    let current_block = current_block - 1;

    // Calculate the block in the current epoch
    let block_in_current_epoch = current_block - first_block_in_epoch;

    // Calculate how much into the epoch we are
    block_in_current_epoch as f64 / blocks_per_epoch as f64
}

// Calculate the time between current epoch and arbitrary epoch
pub fn time_between_epochs(
    blocks_per_epoch: i32,
    current_epoch_progress: f64,
    current_epoch: i32,
    to_epoch: i32,
    epoch_duration: i32,
) -> i32 {
    // This should always return whole number
    let time_per_block = epoch_duration / blocks_per_epoch;

    // But we warn just in case parameters are wrong
    let rest = epoch_duration % blocks_per_epoch;
    if rest != 0 {
        tracing::warn!(
            "Time per block is not a whole number of seconds, time between \
             epoch calculation will be off"
        );
    }

    // Because of the EPIC_SWITCH_BLOCKS_DELAY we need to add some extra time
    let real_epoch_duration =
        epoch_duration + time_per_block * EPOCH_SWITCH_BLOCKS_DELAY as i32;

    let epoch_time = (to_epoch - current_epoch) * real_epoch_duration;
    let extra_time = current_epoch_progress * real_epoch_duration as f64;

    epoch_time - extra_time as i32
}
