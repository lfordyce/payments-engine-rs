# Rust Payments Engine
Simple toy payments engine that reads a series of transactions from a CSV, updates client accounts, handles disputes and chargebacks, and then outputs the state of accounts as a CSV.

## Execution
```shell
cargo run -- transactions.csv > accounts.csv
```
- Alternatively run in release mode:
```shell
cargo run --release -- transactions.csv >accounts.csv
```

## Error Handling
- When an illegal action occurs, for example a transaction attempting to withdrawal more funds than available, the transaction will not be applied to the account and errors will output to `stderr`.
- To capture account balances and errors separately, run the following:
```shell
cargo run -- 2> error.log 1> accounts.csv
```

- JSON logs captured to log file
```shell
cargo run -- etc/process_many.csv --logger json -v 2> error.log 1> accounts.csv
```