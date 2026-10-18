include!("handles.rs");

fn main() {
    let mut registry = dataferry_registry::ConnectorRegistry::new();
    registry.register_state_capable(Db, BackendC);
}
