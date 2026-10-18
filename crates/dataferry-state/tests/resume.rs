//! Resume pins a run to the B1c config hash and the engine version
//! (0003 §10): the golden config is loaded with dataferry-dsl, edited the ways a
//! user would, and resumed against the hash the run was started with.

use std::path::{Path, PathBuf};

use dataferry_dsl::config::{Config, load};
use dataferry_registry::StateStore;
use dataferry_registry::state::{NewRun, RunId};
use dataferry_state::{FileStateStore, ResumeError, resume};

const ENGINE: &str = "0.0.1";
const FLOW: &str = "flows/orders.yaml";

/// A copy of `examples/golden/ferry` to edit.
struct Copy {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
}

impl Copy {
    fn golden() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ferry");
        let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/golden/ferry");
        copy_dir(&golden, &dir);
        Copy { _tmp: tmp, dir }
    }

    fn read(&self, path: &str) -> String {
        std::fs::read_to_string(self.dir.join(path)).unwrap()
    }

    fn write(&self, path: &str, text: &str) {
        let path = self.dir.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    fn edit(&self, path: &str, old: &str, new: &str) {
        let text = self.read(path);
        assert_eq!(text.matches(old).count(), 1, "{path}: {old:?}");
        self.write(path, &text.replace(old, new));
    }

    fn config(&self) -> Config {
        load(&self.dir).unwrap_or_else(|e| panic!("{e}"))
    }

    fn hash(&self) -> String {
        self.config().hash().into_string()
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let target = to.join(path.file_name().unwrap());
        if path.is_dir() {
            copy_dir(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

/// A run started from `copy`'s config as it is now.
async fn start(store: &dyn StateStore, copy: &Copy) -> RunId {
    store
        .create_run(NewRun {
            config_hash: copy.hash(),
            engine_version: ENGINE.into(),
            connections: vec![
                ("shop".into(), "mongo".into()),
                ("warehouse".into(), "postgres".into()),
            ],
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn resume_refuses_an_edited_mapping_naming_both_hashes() {
    let state = tempfile::tempdir().unwrap();
    let store = FileStateStore::new(state.path());
    let copy = Copy::golden();
    let pinned = copy.hash();
    let run = start(&store, &copy).await;

    copy.edit(FLOW, "status: lower(status)", "status: upper(status)");
    let current = copy.hash();
    assert_ne!(current, pinned);

    let err = resume(&store, &run, &current, ENGINE).await.unwrap_err();
    assert_eq!(
        err,
        ResumeError::ConfigChanged {
            run: run.0.clone(),
            pinned: pinned.clone(),
            current: current.clone(),
        }
    );
    let message = err.to_string();
    assert!(message.contains(&pinned), "{message}");
    assert!(message.contains(&current), "{message}");

    // Putting the mapping back makes the run resumable again.
    copy.edit(FLOW, "status: upper(status)", "status: lower(status)");
    resume(&store, &run, &copy.hash(), ENGINE).await.unwrap();
}

#[tokio::test]
async fn resume_refuses_a_changed_behavioural_setting() {
    let state = tempfile::tempdir().unwrap();
    let store = FileStateStore::new(state.path());
    let copy = Copy::golden();
    let run = start(&store, &copy).await;

    copy.edit("runtime.yaml", "max_errors: 10", "max_errors: 11");
    assert!(matches!(
        resume(&store, &run, &copy.hash(), ENGINE).await,
        Err(ResumeError::ConfigChanged { .. })
    ));
}

#[tokio::test]
async fn resume_after_rotating_a_secret_moving_a_flow_or_retuning_succeeds() {
    let state = tempfile::tempdir().unwrap();
    let store = FileStateStore::new(state.path());
    let copy = Copy::golden();
    let run = start(&store, &copy).await;

    // Rotating the password behind `${FERRY_GOLDEN_PG_URL}`: the hash is
    // taken before substitution, so whatever the environment holds the run
    // resumes, and the secret never reaches the state store.
    let config = copy.config();
    for password in ["first-secret", "rotated-secret"] {
        let url = format!("postgres://ferry:{password}@db/warehouse");
        config
            .resolve_with(|name| match name {
                "FERRY_GOLDEN_PG_URL" => Some(url.clone()),
                _ => Some("mongodb://localhost/shop".into()),
            })
            .unwrap();
        let record = resume(&store, &run, config.hash().as_str(), ENGINE)
            .await
            .unwrap();
        assert!(!format!("{record:?}").contains(password));
    }

    // Moving the flow to another file.
    let flow = copy.read(FLOW);
    std::fs::remove_file(copy.dir.join(FLOW)).unwrap();
    copy.write("flows/sales/orders-moved.yml", &flow);
    resume(&store, &run, &copy.hash(), ENGINE).await.unwrap();

    // Raising the tuning settings left out of the hash.
    copy.edit("runtime.yaml", "workers: 2", "workers: 16");
    copy.edit(
        "runtime.yaml",
        "memory_budget: 256MiB",
        "memory_budget: 4GiB",
    );
    let record = resume(&store, &run, &copy.hash(), ENGINE).await.unwrap();
    assert_eq!(record.id, run);

    // And it still holds after a restart: the hash is stored, not cached.
    let restarted = FileStateStore::new(state.path());
    resume(&restarted, &run, &copy.hash(), ENGINE)
        .await
        .unwrap();
}

#[tokio::test]
async fn resume_refuses_another_engine_version_and_unknown_runs() {
    let state = tempfile::tempdir().unwrap();
    let store = FileStateStore::new(state.path());
    let copy = Copy::golden();
    let run = start(&store, &copy).await;

    let err = resume(&store, &run, &copy.hash(), "0.2.0")
        .await
        .unwrap_err();
    assert_eq!(
        err,
        ResumeError::EngineChanged {
            run: run.0.clone(),
            pinned: ENGINE.into(),
            current: "0.2.0".into(),
        }
    );

    let missing = RunId("0123456789abcdef".into());
    assert_eq!(
        resume(&store, &missing, &copy.hash(), ENGINE).await,
        Err(ResumeError::NotFound(missing.0.clone()))
    );
}
