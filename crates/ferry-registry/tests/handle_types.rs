//! A state-capable connector and a state backend must name the same driver
//! handles (0004 §8). The compiler checks it in `register_state_capable`'s
//! `where` clause, so a mismatch fails the build instead of a run.

#[test]
fn mismatched_driver_handles_fail_to_compile() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/matching_handles.rs");
    t.compile_fail("tests/ui/mismatched_conn.rs");
    t.compile_fail("tests/ui/mismatched_pool.rs");
}
