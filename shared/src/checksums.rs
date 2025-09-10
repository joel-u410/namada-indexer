use std::collections::HashMap;

use bimap::BiMap;
use namada_sdk::tx::{
    TX_BECOME_VALIDATOR_WASM, TX_BOND_WASM, TX_BRIDGE_POOL_WASM,
    TX_CHANGE_COMMISSION_WASM, TX_CHANGE_CONSENSUS_KEY_WASM,
    TX_CHANGE_METADATA_WASM, TX_CLAIM_REWARDS_WASM,
    TX_DEACTIVATE_VALIDATOR_WASM, TX_IBC_WASM, TX_INIT_ACCOUNT_WASM,
    TX_INIT_PROPOSAL, TX_REACTIVATE_VALIDATOR_WASM, TX_REDELEGATE_WASM,
    TX_RESIGN_STEWARD, TX_REVEAL_PK, TX_TRANSFER_WASM, TX_UNBOND_WASM,
    TX_UNJAIL_VALIDATOR_WASM, TX_UPDATE_ACCOUNT_WASM,
    TX_UPDATE_STEWARD_COMMISSION, TX_VOTE_PROPOSAL, TX_WITHDRAW_WASM,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Checksums {
    current: BiMap<String, String>,
    fallback: HashMap<String, String>,
}

impl Default for Checksums {
    fn default() -> Self {
        // This hashmap contains historical transactions id -> name
        let mut fallback = HashMap::new();

        // mainnet
        fallback.insert(
            "6d753db0390e7cec16729fc405bfe41384c93bd79f42b8b8be41b22edbbf1b7c"
                .to_string(),
            "tx_transfer".to_string(),
        );
        fallback.insert(
            "cecb1f1b75cd649915423c5e68be20c5232f94ab57a11a908dc66751bbdc4f72"
                .to_string(),
            "tx_ibc".to_string(),
        );
        fallback.insert(
            "b6a1f7e069360650d2c6a1bdd2e5f4e18bb748d35dad02c31c027673fa042d8c"
                .to_string(),
            "tx_claim_rewards".to_string(),
        );
        fallback.insert(
            "b74104949ac0c35ee922fdc3f3db454627742e2483d79550c12fcf31755c6d01"
                .to_string(),
            "tx_claim_rewards".to_string(),
        );
        fallback.insert(
            "ef687f96ec919f5da2e90f125a2800f198a06bcd609a37e5a9ec90d442e32239"
                .to_string(),
            "tx_transfer".to_string(),
        );

        // housefire
        fallback.insert(
            "ef687f96ec919f5da2e90f125a2800f198a06bcd609a37e5a9ec90d442e32239"
                .to_string(),
            "tx_transfer".to_string(),
        );

        fallback.insert(
            "b6a1f7e069360650d2c6a1bdd2e5f4e18bb748d35dad02c31c027673fa042d8c"
                .to_string(),
            "tx_claim_rewards".to_string(),
        );

        fallback.insert(
            "cecb1f1b75cd649915423c5e68be20c5232f94ab57a11a908dc66751bbdc4f72"
                .to_string(),
            "tx_ibc".to_string(),
        );

        fallback.insert(
            "6d753db0390e7cec16729fc405bfe41384c93bd79f42b8b8be41b22edbbf1b7c"
                .to_string(),
            "tx_transfer".to_string(),
        );

        fallback.insert(
            "8a9df03a1a8f5e9e606e14a97fdfb2097dba062da1b3b2158bbfa7deabeeadfb"
                .to_string(),
            "tx_withdraw".to_string(),
        );

        fallback.insert(
            "fb4462b86ff8969826fb66dfa0397cd795df9de9769f7a414d368200a8342951"
                .to_string(),
            "vp_implicit".to_string(),
        );

        fallback.insert(
            "abb163de1f532c740ee6bb69ccc69a5a55505b5c92979d1f256527833b8dfeca"
                .to_string(),
            "vp_user".to_string(),
        );

        fallback.insert(
            "b74104949ac0c35ee922fdc3f3db454627742e2483d79550c12fcf31755c6d01"
                .to_string(),
            "tx_claim_rewards".to_string(),
        );

        fallback.insert(
            "b6a1f7e069360650d2c6a1bdd2e5f4e18bb748d35dad02c31c027673fa042d8c"
                .to_string(),
            "tx_claim_rewards".to_string(),
        );

        fallback.insert(
            "7b0d43f4a277aadd02562d811c755e09d7f191c601ca3bffb89a7f8b599dab1e"
                .to_string(),
            "tx_ibc".to_string(),
        );

        fallback.insert(
            "ef687f96ec919f5da2e90f125a2800f198a06bcd609a37e5a9ec90d442e32239"
                .to_string(),
            "tx_transfer".to_string(),
        );

        fallback.insert(
            "e62fbbbb5de5ddb822439795234bac61e2cdbd70b5cb5015caca17ad210b7f96"
                .to_string(),
            "tx_claim_rewards".to_string(),
        );

        fallback.insert(
            "b74104949ac0c35ee922fdc3f3db454627742e2483d79550c12fcf31755c6d01"
                .to_string(),
            "tx_claim_rewards".to_string(),
        );

        fallback.insert(
            "9a950b45d233c8f6fabfa5c473800c182f7666fe0c35c1a612f66a5f094758fe"
                .to_string(),
            "tx_transfer".to_string(),
        );

        fallback.insert(
            "ef687f96ec919f5da2e90f125a2800f198a06bcd609a37e5a9ec90d442e32239"
                .to_string(),
            "tx_transfer".to_string(),
        );

        fallback.insert(
            "a8686ae87aa940e3c45adb79cb5e1daa71033d41fe85f6b32713ae3cef21ff25"
                .to_string(),
            "tx_withdraw".to_string(),
        );

        fallback.insert(
            "8a9df03a1a8f5e9e606e14a97fdfb2097dba062da1b3b2158bbfa7deabeeadfb"
                .to_string(),
            "tx_withdraw".to_string(),
        );

        fallback.insert(
            "bd1b4230890ff8ece2733de7e770f4a8573a9541813186e5cb718bcc05c2056d"
                .to_string(),
            "tx_ibc".to_string(),
        );

        fallback.insert(
            "7b0d43f4a277aadd02562d811c755e09d7f191c601ca3bffb89a7f8b599dab1e"
                .to_string(),
            "tx_ibc".to_string(),
        );

        Self {
            current: Default::default(),
            fallback,
        }
    }
}

impl Checksums {
    pub fn get_name_by_id(&self, hash: &str) -> Option<String> {
        self.current
            .get_by_right(hash)
            .cloned()
            .or_else(|| self.fallback.get(hash).cloned())
    }

    pub fn add(&mut self, key: String, value: String) {
        let key = key.strip_suffix(".wasm").unwrap().to_owned();
        self.current.insert(key, value);
    }

    pub fn add_with_ext(&mut self, key: String, value: String) {
        self.current.insert(key, value);
    }

    pub fn code_paths() -> Vec<String> {
        vec![
            TX_IBC_WASM.to_string(),
            TX_REVEAL_PK.to_string(),
            TX_TRANSFER_WASM.to_string(),
            TX_BOND_WASM.to_string(),
            TX_REDELEGATE_WASM.to_string(),
            TX_UNBOND_WASM.to_string(),
            TX_WITHDRAW_WASM.to_string(),
            TX_CLAIM_REWARDS_WASM.to_string(),
            TX_VOTE_PROPOSAL.to_string(),
            TX_INIT_PROPOSAL.to_string(),
            TX_CHANGE_METADATA_WASM.to_string(),
            TX_CHANGE_COMMISSION_WASM.to_string(),
            TX_IBC_WASM.to_string(),
            TX_BECOME_VALIDATOR_WASM.to_string(),
            TX_INIT_ACCOUNT_WASM.to_string(),
            TX_UNJAIL_VALIDATOR_WASM.to_string(),
            TX_DEACTIVATE_VALIDATOR_WASM.to_string(),
            TX_REACTIVATE_VALIDATOR_WASM.to_string(),
            TX_UPDATE_ACCOUNT_WASM.to_string(),
            TX_BRIDGE_POOL_WASM.to_string(),
            TX_CHANGE_CONSENSUS_KEY_WASM.to_string(),
            TX_RESIGN_STEWARD.to_string(),
            TX_UPDATE_STEWARD_COMMISSION.to_string(),
        ]
    }
}
