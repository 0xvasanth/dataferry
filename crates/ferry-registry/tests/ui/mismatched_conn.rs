include!("handles.rs");

fn main() {
    let mut registry = ferry_registry::ConnectorRegistry::new();
    registry.register_state_capable(Db, BackendB);
}
