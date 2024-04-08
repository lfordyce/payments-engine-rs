# Rust Payments Engine

Simple toy payments engine that reads a series of transactions from a CSV, updates client accounts, handles disputes and
chargebacks, then outputs the state of accounts as a CSV.

## Usage

### Payments Engine CLI

The program is executed as a simple CLI with a CSV file supplied as an argument

```shell
# outputs results to stdout
cargo run -- transactions.csv
# alternatively outputs piped into a file
cargo run -- transactions.csv > accounts.csv
# or in release mode
cargo run --release -- transactions.csv >accounts.csv
```

### Additional CLI options

```shell
Usage: payments-engine-rs [OPTIONS] <INPUT>

Arguments:
  <INPUT>

Options:
  -v, --verbose...
          Enable debug logs, -vvv for trace [env: PAYMENTS_VERBOSITY=]
      --logger <LOGGER>
          Which logger to use [env: PAYMENTS_LOGGER=] [default: compact] [possible values: compact, full, pretty, json]
      --log-directive [<LOG_DIRECTIVES>...]
          Tracing directives [env: PAYMENTS_LOG_DIRECTIVES=]
  -h, --help
          Print help (see more with '--help')
```

**Error Handling**: When an illegal action occurs, for example a transaction attempting to withdrawal more funds than
available, the transaction will not be applied to the account and errors will be logged to `stderr`
utilizing [tracing](https://docs.rs/tracing/latest/tracing/).

By default, [tracing](https://docs.rs/tracing/latest/tracing/) events are ERROR level. If additional visibility is
required, utilize the following options:

```shell
# utilize env variable with debug log level
PAYMENTS_VERBOSITY=2 cargo run -- etc/transactions.csv
# or with CLI argument
cargo run -- etc/transactions.csv -vv
# capture account balances and errors separately
cargo run -- etc/transactions.csv -vv 2>errors.log 1>accounts.csv
# output logging events, capture accounts to file
cargo run -- etc/transactions.csv -vv 2>&1 > accounts.csv
# change log format
cargo run -- etc/transactions.csv --logger pretty -v 2> error.log 1> accounts.csv
```

## Architecture

The `payments-engine-rs` program uses an Event Sourcing pattern, in which changes to an account state are stored as a
sequence of immutable transaction events. Instead of storing the current state of the account and modifying the account
through updates, event sourcing involves persisting the full series of transaction actions taken on the data. The
advantages of utilizing this architectural pattern are:

- **Complete State History:** Every change is recorded, enabling full traceability and audit trails.
- **Complex Event Reconstruction:** States can be rebuilt or projections created by processing the event log.
- **Improved Scalability** Decouples data commands from updates.
- **Easy Reversion:** The ability to revert to any previous state by replaying events.

Internally, Accounts are represented as Domain Entities or _Aggregates_ and the transaction events or _Domain Events_
are saved to an `In Memory` _Event Store_ (the append-only event log).

## Testing

Run all tests:

```shell
cargo test --all-features
```

Along with unit tests, snapshot testing is supported utilizing [insta](https://docs.rs/insta/1.38.0/insta/)
and [assert_cmd](https://docs.rs/assert_cmd/2.0.14/assert_cmd/)  
