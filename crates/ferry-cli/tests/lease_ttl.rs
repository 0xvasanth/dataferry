//! `FERRY_LEASE_TTL_MS`: how long a killed process's leases hold up a
//! resume. 30 seconds by default; the chaos and conformance suites shorten
//! it to resume at once after every kill -9.

mod common;

use std::io::{BufRead, BufReader};
use std::process::Stdio;
use std::time::{Duration, Instant};

use common::{Shop, Warehouse, edit, ferry, golden_copy, golden_ferry, output};

const REFUSED: &str =
    "FERRY_LEASE_TTL_MS must be a whole number of milliseconds from 1000 to 600000";

#[test]
fn a_lease_that_is_not_a_number_of_milliseconds_in_range_is_a_usage_error() {
    let dir = golden_copy();
    let bad = [
        "",
        "abc",
        "1.5",
        "-1",
        "2",
        // Just outside the range on either side.
        "999",
        "600001",
        // Would overflow the store's interval and the heartbeat's deadline.
        "18446744073709551615",
        // Not even a u64.
        "18446744073709551616",
    ];
    for bad in bad {
        // Refused before the config is even loaded: no database needed.
        let out = output(ferry(dir.path()).env("FERRY_LEASE_TTL_MS", bad).arg("run")).expect(2);
        assert!(out.stderr.contains(REFUSED), "{bad:?}: {}", out.stderr);
        assert!(
            out.stderr.contains("error[F0601]"),
            "{bad:?}: {}",
            out.stderr
        );
        assert!(!out.stderr.contains("W0601"), "{bad:?}: {}", out.stderr);
    }
    // `resume` and `retry` read it too.
    for args in [&["resume", "r1"][..], &["retry", "--failed", "r1"]] {
        let out = output(ferry(dir.path()).env("FERRY_LEASE_TTL_MS", "0").args(args)).expect(2);
        assert!(out.stderr.contains(REFUSED), "{args:?}: {}", out.stderr);
    }
    // Not even text.
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let out = output(
            ferry(dir.path())
                .env("FERRY_LEASE_TTL_MS", std::ffi::OsStr::from_bytes(b"\xff"))
                .arg("run"),
        )
        .expect(2);
        assert!(out.stderr.contains(REFUSED), "{}", out.stderr);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_longest_lease_allowed_reaches_the_store_and_is_warned_about() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    // One partition at a time, slowly, so one stays leased to look at.
    edit(
        &dir.path().join("ferry/runtime.yaml"),
        "  workers: 2\n",
        "  workers: 1\n  source_rows_per_sec: 2\n",
    );
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_ferry"))
        .current_dir(dir.path())
        .env_clear()
        .env("FERRY_GOLDEN_MONGO_URL", &shop.url)
        .env("FERRY_GOLDEN_PG_URL", &warehouse.url)
        .env("FERRY_LEASE_TTL_MS", "600000")
        .args(["-q", "run"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // The lease the store set, in seconds from now: ten minutes, which the
    // heartbeat renews (every 200 s) without overflowing anything.
    let left = "SELECT round(extract(epoch FROM expires_at - now()))::text \
                FROM _ferry_partitions WHERE status = 'leased'";
    let deadline = Instant::now() + Duration::from_secs(60);
    let left = loop {
        let table = warehouse
            .strings("SELECT tablename::text FROM pg_tables WHERE tablename = '_ferry_partitions'")
            .await;
        if !table.is_empty()
            && let Some(left) = warehouse.strings(left).await.pop()
        {
            break left;
        }
        assert!(Instant::now() < deadline, "no partition was leased");
        assert!(child.try_wait().unwrap().is_none(), "the run ended first");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    child.kill().unwrap();
    let out = child.wait_with_output().unwrap();
    let left: i64 = left.parse().unwrap();
    assert!((590..=600).contains(&left), "the lease ends in {left} s");
    // Warned about even under -q.
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(
        stderr.contains(
            "warning[W0601]: FERRY_LEASE_TTL_MS is set: leases last 600000 ms instead of 30 s. \
             For tests only, not safe in production"
        ),
        "{stderr}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resume_after_kill_9_takes_the_dead_processs_partition_once_its_short_lease_ends() {
    let (shop, warehouse) = tokio::join!(Shop::golden(), Warehouse::start());
    let dir = golden_copy();
    let runtime = dir.path().join("ferry/runtime.yaml");
    // One partition at a time, two documents a second: slow enough to kill
    // with a partition leased. Throttles aren't in the config hash.
    edit(
        &runtime,
        "  workers: 2\n",
        "  workers: 1\n  source_rows_per_sec: 2\n",
    );
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_ferry"))
        .current_dir(dir.path())
        .env_clear()
        .env("FERRY_GOLDEN_MONGO_URL", &shop.url)
        .env("FERRY_GOLDEN_PG_URL", &warehouse.url)
        .env("FERRY_LEASE_TTL_MS", "2000")
        .arg("run")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut first = String::new();
    BufReader::new(child.stdout.as_mut().unwrap())
        .read_line(&mut first)
        .unwrap();
    let run = first.trim().strip_prefix("run ").unwrap().to_owned();
    // A partition is leased: the run holds work it hasn't committed.
    let deadline = Instant::now() + Duration::from_secs(60);
    let leased = format!(
        "SELECT count(*)::text FROM _ferry_partitions WHERE run_id = '{run}' AND status = 'leased'"
    );
    while warehouse
        .client
        .query(
            "SELECT 1 FROM pg_tables WHERE tablename = '_ferry_partitions'",
            &[],
        )
        .await
        .unwrap()
        .is_empty()
        || warehouse.strings(&leased).await != ["1"]
    {
        assert!(Instant::now() < deadline, "no partition was leased");
        assert!(child.try_wait().unwrap().is_none(), "the run ended first");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    child.kill().unwrap(); // SIGKILL: the lease stays held
    child.wait().unwrap();

    edit(&runtime, "  source_rows_per_sec: 2\n", "");
    let started = Instant::now();
    let out = output(
        golden_ferry(dir.path(), &shop, &warehouse)
            .env("FERRY_LEASE_TTL_MS", "2000")
            .args(["resume", &run]),
    )
    .expect(0)
    .no_secret();
    let took = started.elapsed();
    // The override is used, and said to be.
    assert!(
        out.stderr
            .contains("warning[W0601]: FERRY_LEASE_TTL_MS is set: leases last 2000 ms"),
        "{}",
        out.stderr
    );
    assert!(
        out.stdout.contains("verification of `warehouse` passed"),
        "{}",
        out.stdout
    );
    // It waited for the lease (at most two seconds), not the default 30.
    assert!(took < Duration::from_secs(15), "the resume took {took:?}");
    assert_eq!(warehouse.count("public.orders").await, 8);
}
