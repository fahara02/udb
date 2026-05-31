use udb::{Backend, BackendKind, BackendPluginContract, BackendPluginSurface};

pub struct ToyBackendPlugin;

#[async_trait::async_trait]
impl Backend for ToyBackendPlugin {
    fn kind(&self) -> BackendKind {
        BackendKind::Sqlite
    }

    fn contract(&self) -> BackendPluginContract {
        let mut contract = BackendPluginContract::default_for(self.kind());
        contract.executor = BackendPluginSurface::required(
            "toy_backend::Executor",
            "Example executor owned by the external backend crate",
        );
        contract
    }
}

pub static TOY_BACKEND_PLUGIN: ToyBackendPlugin = ToyBackendPlugin;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toy_plugin_satisfies_udb_contract() {
        assert!(TOY_BACKEND_PLUGIN.conformance_report().passed);
    }
}
