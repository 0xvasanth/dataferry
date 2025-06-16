use std::process::ExitCode;

use ferry_cli::Ferry;

fn main() -> ExitCode {
    Ferry::with_builtins().main()
}
