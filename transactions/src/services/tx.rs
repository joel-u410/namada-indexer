use bigdecimal::BigDecimal;
use namada_sdk::address::Address;
use namada_sdk::hash::Hash;
use namada_sdk::ibc::core::channel::types::acknowledgement::AcknowledgementStatus;
use namada_sdk::ibc::core::channel::types::msgs::PacketMsg;
use namada_sdk::ibc::core::handler::types::msgs::MsgEnvelope;
use shared::block_result::{BlockResult, TxAttributesType};
use shared::gas::GasEstimation;
use shared::id::Id;
use shared::transaction::{
    IbcAck, IbcAckStatus, IbcSequence, IbcTokenAction, InnerTransaction,
    TransactionKind, WrapperTransaction, ibc_denom_received, ibc_denom_sent,
};

pub fn get_ibc_token_flows(
    block_results: &BlockResult,
) -> impl Iterator<Item = (IbcTokenAction, String, BigDecimal)> + use<'_> {
    block_results.end_events.iter().filter_map(|event| {
        let (action, original_packet, fungible_token_packet) =
            event.attributes.as_ref()?.as_fungible_token_packet()?;

        let denom = match &action {
            IbcTokenAction::Withdraw => {
                ibc_denom_sent(&fungible_token_packet.denom)
            }
            IbcTokenAction::Deposit => {
                let packet = original_packet?;

                ibc_denom_received(
                    &fungible_token_packet.denom,
                    &packet.source_port,
                    &packet.source_channel,
                    &packet.dest_port,
                    &packet.dest_channel,
                )
                .inspect_err(|err| {
                    tracing::debug!(?err, "Failed to parse received IBC denom");
                })
                .ok()?
            }
        };

        Some((action, denom, fungible_token_packet.amount.clone()))
    })
}

pub fn get_ibc_packets(
    block_results: &BlockResult,
    txs: &[(WrapperTransaction, Vec<InnerTransaction>)],
) -> Vec<IbcSequence> {
    let mut legacy_extracted_id_tx_ids =
        txs.iter().flat_map(|(wrapper_tx, inner_txs)| {
            let mut inner_txs_it = inner_txs.iter();

            std::iter::from_fn(move || {
                let inner_tx = inner_txs_it.next()?;

                // Extract successful ibc transactions from each batch
                if inner_tx.is_sent_ibc() && inner_tx.was_successful(wrapper_tx)
                {
                    Some(inner_tx.tx_id.to_owned())
                } else {
                    None
                }
            })
        });

    block_results
        .end_events
        .iter()
        .filter_map(|event| {
            if let Some(attributes) = &event.attributes {
                match attributes {
                    TxAttributesType::SendPacket(packet) => Some(IbcSequence {
                        sequence_number: packet.sequence.clone(),
                        source_port: packet.source_port.clone(),
                        dest_port: packet.dest_port.clone(),
                        source_channel: packet.source_channel.clone(),
                        dest_channel: packet.dest_channel.clone(),
                        timeout: packet.timeout_timestamp,
                        tx_id: {
                            if let Some(id) = event.inner_tx_hash.as_ref() {
                                // the id was in the event. this should
                                // be the case 99% of the times, unless
                                // we're crawling through the history
                                // of some older namada version, or
                                // we encounter a pgf funding tx
                                id.clone()
                            } else if packet
                                .as_fungible_token_packet()
                                .is_some_and(|ics20_packet| {
                                    matches!(
                                        ics20_packet.sender.parse().ok(),
                                        Some(Address::Internal(_))
                                    )
                                })
                            {
                                // this packet was sent by an internal address,
                                // most likely the pgf (for pgf funding via
                                // IBC). there is no inner tx id in this
                                // case, let's add the hash of the packet
                                // id, as a workaround.
                                Id::Hash(
                                    Hash::sha256(packet.id())
                                        .to_string()
                                        .to_lowercase(),
                                )
                            } else {
                                // this handles older namada versions
                                legacy_extracted_id_tx_ids.next().expect(
                                    "Ibc sent packet should have a \
                                     corresponding tx",
                                )
                            }
                        },
                    }),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
}

pub fn get_ibc_ack_packet(inner_txs: &[InnerTransaction]) -> Vec<IbcAck> {
    inner_txs.iter().filter_map(|tx| match tx.kind.clone() {
        TransactionKind::IbcMsg(Some(ibc_message)) => match ibc_message.0 {
            namada_sdk::ibc::IbcMessage::Envelope(msg_envelope) => {
                match *msg_envelope {
                    MsgEnvelope::Packet(packet_msg) => match packet_msg {
                        PacketMsg::Recv(_) => None,
                        PacketMsg::Ack(msg) => {
                            let ack = match serde_json::from_slice::<
                                AcknowledgementStatus,
                            >(
                                msg.acknowledgement.as_bytes()
                            ) {
                                Ok(status) => IbcAck {
                                    sequence_number: msg.packet.seq_on_a.to_string(),
                                    source_port: msg.packet.port_id_on_a.to_string(),
                                    dest_port: msg.packet.port_id_on_b.to_string(),
                                    source_channel: msg.packet.chan_id_on_a.to_string(),
                                    dest_channel: msg.packet.chan_id_on_b.to_string(),
                                    status: match status {
                                        AcknowledgementStatus::Success(_) => IbcAckStatus::Success,
                                        AcknowledgementStatus::Error(_) => IbcAckStatus::Fail,
                                    },
                                },
                                Err(_) => IbcAck {
                                    sequence_number: msg.packet.seq_on_a.to_string(),
                                    source_port: msg.packet.port_id_on_a.to_string(),
                                    dest_port: msg.packet.port_id_on_b.to_string(),
                                    source_channel: msg.packet.chan_id_on_a.to_string(),
                                    dest_channel: msg.packet.chan_id_on_b.to_string(),
                                    status: IbcAckStatus::Unknown,
                                },
                            };
                            Some(ack)
                        }
                        PacketMsg::Timeout(msg) => Some(IbcAck {
                            sequence_number: msg.packet.seq_on_a.to_string(),
                            source_port: msg.packet.port_id_on_a.to_string(),
                            dest_port: msg.packet.port_id_on_b.to_string(),
                            source_channel: msg.packet.chan_id_on_a.to_string(),
                            dest_channel: msg.packet.chan_id_on_b.to_string(),
                            status: IbcAckStatus::Timeout,
                        }),
                        PacketMsg::TimeoutOnClose(msg) => Some(IbcAck {
                            sequence_number: msg.packet.seq_on_a.to_string(),
                            source_port: msg.packet.port_id_on_a.to_string(),
                            dest_port: msg.packet.port_id_on_b.to_string(),
                            source_channel: msg.packet.chan_id_on_a.to_string(),
                            dest_channel: msg.packet.chan_id_on_b.to_string(),
                            status: IbcAckStatus::Timeout,
                        }),
                    },
                    _ => None,
                }
            },
            _ => None
        },
        _ => None,
    }).collect()
}

pub fn get_gas_estimates(
    txs: &[(WrapperTransaction, Vec<InnerTransaction>)],
) -> Vec<GasEstimation> {
    txs.iter()
        .filter(|(wrapper_tx, inner_txs)| {
            inner_txs
                .iter()
                // We can only index gas if all the inner transactions of the
                // batch were successfully executed, otherwise we'd end up
                // inserting in the db a gas value which is not guaranteed to be
                // enough for such a batch
                .all(|inner_tx| inner_tx.was_successful(wrapper_tx))
        })
        .map(|(wrapper_tx, inner_txs)| {
            let mut gas_estimate = GasEstimation::new(wrapper_tx.tx_id.clone());
            gas_estimate.signatures = wrapper_tx.total_signatures;
            gas_estimate.size = wrapper_tx.size;

            inner_txs.iter().for_each(|tx| match tx.kind {
                TransactionKind::TransparentTransfer(_) => {
                    gas_estimate.increase_transparent_transfer();
                }
                TransactionKind::MixedTransfer(_) => {
                    let notes = tx.notes;
                    gas_estimate.increase_mixed_transfer(notes)
                }
                TransactionKind::IbcSendTrasparentTransfer(_)
                | TransactionKind::IbcRecvTrasparentTransfer(_) => {
                    gas_estimate.increase_ibc_transparent_transfer()
                }
                TransactionKind::Bond(_) => gas_estimate.increase_bond(),
                TransactionKind::Redelegation(_) => {
                    gas_estimate.increase_redelegation()
                }
                TransactionKind::Unbond(_) => gas_estimate.increase_unbond(),
                TransactionKind::Withdraw(_) => {
                    gas_estimate.increase_withdraw()
                }
                TransactionKind::ClaimRewards(_) => {
                    gas_estimate.increase_claim_rewards()
                }
                TransactionKind::ProposalVote(_) => {
                    gas_estimate.increase_vote()
                }
                TransactionKind::RevealPk(_) => {
                    gas_estimate.increase_reveal_pk()
                }
                TransactionKind::ShieldedTransfer(_) => {
                    let notes = tx.notes;
                    gas_estimate.increase_shielded_transfer(notes);
                }
                TransactionKind::ShieldingTransfer(_) => {
                    let notes = tx.notes;
                    gas_estimate.increase_shielding_transfer(notes)
                }
                TransactionKind::UnshieldingTransfer(_) => {
                    let notes = tx.notes;
                    gas_estimate.increase_unshielding_transfer(notes)
                }
                TransactionKind::IbcShieldingTransfer(_) => {
                    let notes = tx.notes;
                    gas_estimate.increase_ibc_shielding_transfer(notes)
                }
                TransactionKind::IbcUnshieldingTransfer(_) => {
                    let notes = tx.notes;
                    gas_estimate.increase_ibc_unshielding_transfer(notes)
                }
                TransactionKind::ChangeConsensusKey(_)
                | TransactionKind::IbcMsg(_)
                | TransactionKind::InitAccount(_)
                | TransactionKind::InitProposal(_)
                | TransactionKind::MetadataChange(_)
                | TransactionKind::CommissionChange(_)
                | TransactionKind::BecomeValidator(_)
                | TransactionKind::ReactivateValidator(_)
                | TransactionKind::DeactivateValidator(_)
                | TransactionKind::UnjailValidator(_)
                | TransactionKind::Unknown(_) => (),
            });
            gas_estimate
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use namada_sdk::address::PGF;
    use namada_sdk::ibc::apps::transfer::types::PrefixedCoin;
    use namada_sdk::ibc::apps::transfer::types::packet::PacketData as Ics20PacketData;
    use shared::block_result::{
        Event, EventKind, IbcCorePacketKind, IbcPacket,
    };
    use shared::ser::{AccountsMap, TransferData};
    use shared::token::Token;
    use shared::transaction::TransactionExitStatus;

    use super::*;

    fn mock_block_result(
        inner_tx_hash: Option<&str>,
        with_packet_sender: Option<String>,
    ) -> BlockResult {
        BlockResult {
            end_events: vec![Event {
                kind: EventKind::IbcCore(IbcCorePacketKind::Send),
                inner_tx_hash: inner_tx_hash
                    .map(|hash| Id::Hash(hash.to_string())),
                attributes: Some(TxAttributesType::SendPacket(IbcPacket {
                    source_port: "transfer".to_string(),
                    dest_port: "transfer".to_string(),
                    source_channel: "channel-0".to_string(),
                    dest_channel: "channel-0".to_string(),
                    timeout_timestamp: 0,
                    timeout_height: String::new(),
                    sequence: "1".to_string(),
                    data: with_packet_sender
                        .map(|sender| {
                            serde_json::to_string(&Ics20PacketData {
                                token: PrefixedCoin {
                                    denom: "eatshit".parse().unwrap(),
                                    amount: "1234".parse().unwrap(),
                                },
                                sender: sender.into(),
                                receiver: "a1aaaa".to_string().into(),
                                memo: String::new().into(),
                            })
                            .unwrap()
                        })
                        .unwrap_or_default(),
                })),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn test_get_ibc_packets() {
        let expected_seq = |tx_id| IbcSequence {
            sequence_number: "1".to_string(),
            source_port: "transfer".to_string(),
            dest_port: "transfer".to_string(),
            source_channel: "channel-0".to_string(),
            dest_channel: "channel-0".to_string(),
            timeout: 0,
            tx_id,
        };

        // get ibc seq just from the events + inner tx hash
        let block_result = mock_block_result(Some("deadbeef"), None);
        assert_eq!(
            get_ibc_packets(&block_result, &[]),
            vec![expected_seq(Id::Hash("deadbeef".to_string()))],
        );

        // protocol transfer, there is no inner tx hash
        let block_result = mock_block_result(None, Some(PGF.to_string()));
        assert_eq!(
            get_ibc_packets(&block_result, &[]),
            vec![expected_seq(Id::Hash(
                Hash::sha256("transfer/channel-0/transfer/channel-0/1")
                    .to_string()
                    .to_lowercase()
            ))],
        );

        // no inner tx hash in the event, get it from the provided tx slice
        let block_result = mock_block_result(None, Some("a1aaaa".to_string()));
        let wrapper = WrapperTransaction {
            exit_code: TransactionExitStatus::Applied,
            tx_id: Id::Hash("eatshit".to_string()),
            index: 0,
            fee: Default::default(),
            atomic: false,
            block_height: 0,
            total_signatures: 0,
            size: 0,
        };
        let inner = InnerTransaction {
            tx_id: Id::Hash("deadbeef".to_string()),
            wrapper_id: Id::Hash("eatshit".to_string()),
            index: 0,
            kind: TransactionKind::IbcSendTrasparentTransfer((
                Token::Native(Id::Hash("aabbcc".to_string())),
                TransferData {
                    sources: AccountsMap(Default::default()),
                    targets: AccountsMap(Default::default()),
                    shielded_section_hash: None,
                },
            )),
            data: None,
            extra_sections: Default::default(),
            memo: None,
            notes: 0,
            exit_code: TransactionExitStatus::Applied,
        };
        assert_eq!(
            get_ibc_packets(&block_result, &[(wrapper, vec![inner])]),
            vec![expected_seq(Id::Hash("deadbeef".to_string()))],
        );
    }
}
