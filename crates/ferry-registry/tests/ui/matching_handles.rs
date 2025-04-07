// Control for the mismatched cases: the same connector with a backend on
// the same driver handles compiles.

include!("handles.rs");

fn main() {
    let mut registry = ferry_registry::ConnectorRegistry::new();
    registry.register_state_capable(Db, BackendA);
}
