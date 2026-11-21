**Earn SOL by running a LastBuy Keeper. Receive rent exemption and unused lamports from migration funding for successfully executing permissionless migrations.**

LastBuy Keeper executes permissionless bonding curve migrations.

Requires Rust, a funded Solana keypair, a v1-capable RPC and a Yellowstone gRPC endpoint.

```sh
cargo build --release --locked
./target/release/keeper init
# Add RPC/gRPC URLs and the optional token to keeper.secrets.toml.
./target/release/keeper check
./target/release/keeper run
```

Minimal configuration:

```toml
keypair_path = "~/.config/solana/id.json"
```

`keeper init` creates `keeper.toml` for ordinary settings and a private, Git-ignored `keeper.secrets.toml` for RPC/gRPC URLs and the optional token. `KEEPER_RPC_URL`, `KEEPER_GRPC_URL`, and `KEEPER_GRPC_X_TOKEN` override those secret values. Keep the secrets file on a trusted machine and do not share it.

Use `keeper profit --config keeper.toml` to view realized net profit and failed transaction costs. Accounting uses finalized transaction balances and persists in the configured state directory.

Use `keeper reset-ledger --config keeper.toml` to permanently remove the active ledger. This discards its accounting, pending attempt history, and durable submission exclusions. The next `keeper run` initializes a fresh ledger.
