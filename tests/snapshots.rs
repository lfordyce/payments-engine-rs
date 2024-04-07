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

#[test]
fn trigger_locked() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("payments-engine-rs")?;
    cmd.arg("./etc/locked.csv");
    let stdout = String::from_utf8(cmd.assert().success().get_output().stdout.clone())?;

    insta::assert_snapshot!(stdout);

    Ok(())
}

#[test]
fn dispute_deposit() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("payments-engine-rs")?;
    cmd.arg("./etc/dispute_deposit.csv");
    let stdout = String::from_utf8(cmd.assert().success().get_output().stdout.clone())?;

    insta::assert_snapshot!(stdout);

    Ok(())
}

#[test]
fn deposit_chargeback() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("payments-engine-rs")?;
    cmd.arg("./etc/deposit_chargeback.csv");
    let stdout = String::from_utf8(cmd.assert().success().get_output().stdout.clone())?;

    insta::assert_snapshot!(stdout);

    Ok(())
}

#[test]
fn funds_held() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("payments-engine-rs")?;
    cmd.arg("./etc/funds_held.csv");
    let stdout = String::from_utf8(cmd.assert().success().get_output().stdout.clone())?;

    insta::assert_snapshot!(stdout);

    Ok(())
}

#[test]
fn smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("payments-engine-rs")?;
    cmd.arg("./etc/smoketest.csv");
    let stdout = String::from_utf8(cmd.assert().success().get_output().stdout.clone())?;

    insta::assert_snapshot!(stdout);

    Ok(())
}
