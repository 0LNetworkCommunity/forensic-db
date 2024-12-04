use crate::schema_transaction::{EntryFunctionArgs, RelationLabel, UserEventTypes, WarehouseEvent};
use anyhow::bail;
use diem_types::transaction::SignedTransaction;
use libra_backwards_compatibility::sdk::{
    v6_libra_framework_sdk_builder::EntryFunctionCall as V6EntryFunctionCall,
    v7_libra_framework_sdk_builder::EntryFunctionCall as V7EntryFunctionCall,
};
// use libra_cached_packages::libra_stdlib::EntryFunctionCall as CurrentVersionEntryFunctionCall;

/// test all entry function decoders for the current bytes
pub fn decode_entry_function_all_versions(
    user_tx: &SignedTransaction,
    events: &[WarehouseEvent],
) -> anyhow::Result<(EntryFunctionArgs, RelationLabel)> {
    // TODO: current version encoding

    // if let Some((args, relation)) = maybe_get_current_version_relation(user_tx, events) {
    //     return Ok((args, relation));
    // }

    if let Some((ef, rel)) = maybe_get_v7_relation(user_tx, events) {
        return Ok((ef, rel));
    }

    if let Some((ef, rel)) = maybe_get_v6_relation(user_tx, events) {
        return Ok((ef, rel));
    }

    bail!("no entry function found")
}

// TODO: the CurrentVersionEntryFunctionCall needs serde derives
// Using HEAD libra-framework code base try to decode transaction
// fn maybe_get_current_version_relation(
//     user_tx: &SignedTransaction,
//     events: &[WarehouseEvent],
// ) -> Option<(EntryFunctionArgs, RelationLabel)> {
//     let ef = CurrentVersionEntryFunctionCall::decode(user_tx.payload());

//     let relation = match ef {
//         Some(EntryFunctionCall::OlAccountTransfer { to, amount: _ }) => {
//             if is_onboarding_event(events) {
//                 RelationLabel::Onboarding(to)
//             } else {
//                 RelationLabel::Transfer(to)
//             }
//         }
//         Some(EntryFunctionCall::OlAccountCreateAccount { auth_key }) => {
//             RelationLabel::Onboarding(auth_key)
//         }
//         Some(EntryFunctionCall::VouchVouchFor { friend_account }) => {
//             RelationLabel::Vouch(friend_account)
//         }
//         Some(EntryFunctionCall::VouchInsistVouchFor { friend_account }) => {
//             RelationLabel::Vouch(friend_account)
//         }
//         Some(EntryFunctionCall::CoinTransfer { to, .. }) => RelationLabel::Transfer(to),
//         Some(EntryFunctionCall::AccountRotateAuthenticationKeyWithRotationCapability {
//             rotation_cap_offerer_address,
//             ..
//         }) => RelationLabel::Transfer(rotation_cap_offerer_address),

//         // TODO: get other entry functions with known counter parties
//         // if nothing is found try to decipher from events
//         _ => return None,
//     };

//     let args = EntryFunctionArgs::Current(ef.unwrap());

//     Some((args, relation))
// }

fn maybe_get_v7_relation(
    user_tx: &SignedTransaction,
    events: &[WarehouseEvent],
) -> Option<(EntryFunctionArgs, RelationLabel)> {
    let ef = V7EntryFunctionCall::decode(user_tx.payload());

    let relation = match ef {
        Some(V7EntryFunctionCall::OlAccountTransfer { to, amount: _ }) => {
            if is_onboarding_event(events) {
                RelationLabel::Onboarding(to)
            } else {
                RelationLabel::Transfer(to)
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
        Some(V7EntryFunctionCall::CoinTransfer { to, .. }) => RelationLabel::Transfer(to),
        Some(V7EntryFunctionCall::AccountRotateAuthenticationKeyWithRotationCapability {
            rotation_cap_offerer_address,
            ..
        }) => RelationLabel::Transfer(rotation_cap_offerer_address),

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
        Some(V6EntryFunctionCall::OlAccountTransfer { to, amount: _ }) => {
            if is_onboarding_event(events) {
                RelationLabel::Onboarding(to)
            } else {
                RelationLabel::Transfer(to)
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
        Some(V6EntryFunctionCall::CoinTransfer { to, .. }) => RelationLabel::Transfer(to),
        Some(V6EntryFunctionCall::AccountRotateAuthenticationKeyWithRotationCapability {
            rotation_cap_offerer_address,
            ..
        }) => RelationLabel::Transfer(rotation_cap_offerer_address),

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
