use crate::collectors::functions::WalFunctionsRow;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbRole {
    Primary,
    Standby,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum EventSnapshot {
    Disconnected,
    Reconnected,
    RoleChanged { old: DbRole, new: DbRole },
}

#[derive(Debug, Default)]
pub struct RuntimeState {
    pub connected: bool,
    pub role: Option<DbRole>,
}

pub fn on_disconnect(state: &mut RuntimeState) -> Option<EventSnapshot> {
    if state.connected {
        state.connected = false;
        Some(EventSnapshot::Disconnected)
    } else {
        None
    }
}

pub fn on_reconnect(state: &mut RuntimeState) -> Option<EventSnapshot> {
    if !state.connected {
        state.connected = true;
        Some(EventSnapshot::Reconnected)
    } else {
        None
    }
}

pub fn on_role_observed(state: &mut RuntimeState, wal_functions: &WalFunctionsRow) -> Option<EventSnapshot> {
    let new_role = if wal_functions.is_in_recovery { DbRole::Standby } else { DbRole::Primary };

    match state.role {
        Some(old_role) if old_role != new_role => {
            state.role = Some(new_role);
            Some(EventSnapshot::RoleChanged { old: old_role, new: new_role })
        }
        None => {
            state.role = Some(new_role);
            None
        }
        _ => None,
    }
}
