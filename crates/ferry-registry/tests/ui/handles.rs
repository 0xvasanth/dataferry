// Shared by the handle-type UI tests: a state-capable connector on
// `DriverA`'s handles. Not a test on its own.

use std::sync::Arc;

use ferry_registry::{
    BuildContext, BuildError, Connector, Endpoints, Ledger, Roles, StateBackend, StateCapable,
    StateStore,
};

#[derive(Clone)]
pub struct PoolA;
pub struct ConnA;

#[derive(Clone)]
pub struct PoolB;
pub struct ConnB;

#[derive(serde::Deserialize, schemars::JsonSchema)]
pub struct Options {}

pub struct Db;

impl Connector for Db {
    const TYPE: &'static str = "db";
    const ROLES: Roles = Roles::SINK;
    type Options = Options;

    fn build(&self, _: &BuildContext<'_>, _: Options, _: Roles) -> Result<Endpoints, BuildError> {
        unimplemented!()
    }

    fn describe(&self, _: &Options) -> String {
        unimplemented!()
    }
}

impl StateCapable for Db {
    type Pool = PoolA;
    type Conn = ConnA;

    fn pool(&self, _: &BuildContext<'_>, _: &Options) -> Result<PoolA, BuildError> {
        unimplemented!()
    }

    fn build_with_ledger(
        &self,
        _: &BuildContext<'_>,
        _: Options,
        _: Roles,
        _: PoolA,
        _: Arc<dyn Ledger<ConnA>>,
    ) -> Result<Endpoints, BuildError> {
        unimplemented!()
    }
}

/// A backend written against the same driver.
pub struct BackendA;

impl StateBackend for BackendA {
    type Pool = PoolA;
    type Conn = ConnA;

    fn open(&self, _: PoolA) -> (Arc<dyn StateStore>, Arc<dyn Ledger<ConnA>>) {
        unimplemented!()
    }
}

/// The same pool type, but a different connection type: e.g. the state
/// crate built against another major version of the driver.
pub struct BackendB;

impl StateBackend for BackendB {
    type Pool = PoolA;
    type Conn = ConnB;

    fn open(&self, _: PoolA) -> (Arc<dyn StateStore>, Arc<dyn Ledger<ConnB>>) {
        unimplemented!()
    }
}

/// Neither handle matches.
pub struct BackendC;

impl StateBackend for BackendC {
    type Pool = PoolB;
    type Conn = ConnB;

    fn open(&self, _: PoolB) -> (Arc<dyn StateStore>, Arc<dyn Ledger<ConnB>>) {
        unimplemented!()
    }
}
