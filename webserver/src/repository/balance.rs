use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl, SelectableHelper};
use orm::balances::BalanceDb;
use orm::schema::token;
use orm::token::TokenDb;
use orm::views::balances;

use crate::appstate::AppState;

#[derive(Clone)]
pub struct BalanceRepo {
    pub(crate) app_state: AppState,
}

pub trait BalanceRepoTrait {
    fn new(app_state: AppState) -> Self;

    async fn get_all_token(&self) -> Result<Vec<TokenDb>, String>;

    async fn get_address_balances(
        &self,
        address: String,
    ) -> Result<Vec<BalanceDb>, String>;
}

impl BalanceRepoTrait for BalanceRepo {
    fn new(app_state: AppState) -> Self {
        Self { app_state }
    }

    async fn get_address_balances(
        &self,
        address: String,
    ) -> Result<Vec<BalanceDb>, String> {
        let conn = self.app_state.get_db_connection().await;

        conn.interact(move |conn| {
            balances::table
                .filter(balances::dsl::owner.eq(address))
                .select(BalanceDb::as_select())
                .get_results(conn)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
    }

    async fn get_all_token(&self) -> Result<Vec<TokenDb>, String> {
        let conn = self.app_state.get_db_connection().await;

        conn.interact(move |conn| {
            token::table
                .distinct()
                .select(TokenDb::as_select())
                .get_results(conn)
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
    }
}
