# AGENTS.md

## Project Overview

OpenData is a collection of deployable database systems that share common infrastructure. Each database is its own crate but leverages shared logic from `opendata-common`.

### Crates

- **opendata-common**: Shared library containing common utilities and abstractions used by all database implementations
- **open-tsdb**: A timeseries database optimized for time-ordered data
- **open-log**: A Kafka-like log abstraction for append-only event streams

## Development

### Building

```bash
cargo build              # Build all crates
cargo build -p open-tsdb # Build specific crate
```

### Testing

```bash
cargo test --all         # Run all tests
cargo test -p open-tsdb  # Test specific crate
```

### Formatting and Linting

Always run these before committing:

```bash
cargo fmt                # Format code
cargo clippy             # Run lints
```

**Important**: After modifying any `.rs` file, run `cargo fmt`.

## Code Conventions

### Tests

- Use the **given/when/then** pattern for test structure
- Name tests `should_xyz` to describe expected behavior

```rust
#[test]
fn should_return_error_when_key_not_found() {
    // given
    let store = Store::new();

    // when
    let result = store.get("missing_key");

    // then
    assert!(result.is_err());
}
```

### Dependencies

- **slatedb**: The underlying storage engine for all database implementations
- Workspace dependencies are defined in the root `Cargo.toml`
- Prefer adding dependencies at the workspace level when shared across crates

### Adding a New Crate

1. Create the crate directory with `Cargo.toml` and `src/`
2. Add the crate name to `members` in the root `Cargo.toml`
3. Use `version.workspace = true` and `edition.workspace = true`
4. Add shared dependencies with `.workspace = true`
