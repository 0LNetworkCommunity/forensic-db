use crate::schema_transaction::{EntryFunctionArgs, RelationLabel, UserEventTypes, WarehouseEvent};
use anyhow::bail;
use diem_types::transaction::SignedTransaction;
use libra_backwards_compatibility::sdk::{
    v6_libra_framework_sdk_builder::EntryFunctionCall as V6EntryFunctionCall,
    v7_libra_framework_sdk_builder::EntryFunctionCall as V7EntryFunctionCall,
};

/// EntryFuntion decoding for V6, V7 eras
pub fn decode_entry_function_all_versions(
    user_tx: &SignedTransaction,
    events: &[WarehouseEvent],
) -> anyhow::Result<(EntryFunctionArgs, RelationLabel)> {
    if let Some((ef, rel)) = maybe_get_v7_relation(user_tx, events) {
        return Ok((ef, rel));
    }

    if let Some((ef, rel)) = maybe_get_v6_relation(user_tx, events) {
        return Ok((ef, rel));
    }

    bail!("no entry function found")
}

fn maybe_get_v7_relation(
    user_tx: &SignedTransaction,
    events: &[WarehouseEvent],
) -> Option<(EntryFunctionArgs, RelationLabel)> {
    let ef = V7EntryFunctionCall::decode(user_tx.payload());

    let relation = match ef {
        Some(V7EntryFunctionCall::OlAccountTransfer { to, amount }) => {
            if is_onboarding_event(events) {
                RelationLabel::Onboarding(to)
            } else {
                RelationLabel::Transfer(to, amount)
            }
        }
        Some(V7EntryFunctionCall::OlAccountCreateAccount { auth_key }) => {
            RelationLabel::Onboarding(auth_key)
        }
        Some(V7EntryFunctionCall::VouchVouchFor { friend_account }) => {
            RelationLabel::Vouch(friend_account)
        }
        Some(V7EntryFunctionCall::VouchInsistVouchFor { friend_account }) => {
            RelationLabel::Vouch(friend_account)
        }
        Some(V7EntryFunctionCall::CoinTransfer { to, amount, .. }) => {
            RelationLabel::Transfer(to, amount)
        }

        // TODO: get other entry functions with known counter parties
        // if nothing is found try to decipher from events
        _ => return None,
    };

    let args = EntryFunctionArgs::V7(ef.unwrap());

    Some((args, relation))
}

fn maybe_get_v6_relation(
    user_tx: &SignedTransaction,
    events: &[WarehouseEvent],
) -> Option<(EntryFunctionArgs, RelationLabel)> {
    let ef = V6EntryFunctionCall::decode(user_tx.payload());
    let relation = match ef {
        Some(V6EntryFunctionCall::OlAccountTransfer { to, amount }) => {
            if is_onboarding_event(events) {
                RelationLabel::Onboarding(to)
            } else {
                RelationLabel::Transfer(to, amount)
            }
        }
        Some(V6EntryFunctionCall::OlAccountCreateAccount { auth_key }) => {
            RelationLabel::Onboarding(auth_key)
        }
        Some(V6EntryFunctionCall::VouchVouchFor { wanna_be_my_friend }) => {
            RelationLabel::Vouch(wanna_be_my_friend)
        }
        Some(V6EntryFunctionCall::VouchInsistVouchFor { wanna_be_my_friend }) => {
            RelationLabel::Vouch(wanna_be_my_friend)
        }
        Some(V6EntryFunctionCall::CoinTransfer { to, amount, .. }) => {
            RelationLabel::Transfer(to, amount)
        }

        // TODO: get other entry functions with known counter parties
        // if nothing is found try to decipher from events
        _ => return None,
    };
    let args = EntryFunctionArgs::V6(ef.unwrap());

    Some((args, relation))
}

fn is_onboarding_event(events: &[WarehouseEvent]) -> bool {
    let withdraw = events.iter().any(|e| {
        if let UserEventTypes::Withdraw(_) = e.event {
            return true;
        }
        false
    });

    let deposit = events.iter().any(|e| {
        if let UserEventTypes::Deposit(_) = e.event {
            return true;
        }
        false
    });

    let onboard = events.iter().any(|e| {
        if let UserEventTypes::Onboard(_) = e.event {
            return true;
        }
        false
    });

    withdraw && deposit && onboard
}
