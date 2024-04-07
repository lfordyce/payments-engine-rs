use assert_cmd::prelude::*;
use std::process::Command;
use std::str;

#[test]
fn basic() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("payments-engine-rs")?;
    cmd.arg("./etc/basic.csv");
    let stdout = String::from_utf8(cmd.assert().success().get_output().stdout.clone())?;

    insta::assert_snapshot!(stdout);

    Ok(())
}
