use std::process::ExitCode;

use dataferry::Ferry;

fn main() -> ExitCode {
    Ferry::with_builtins().main()
}
