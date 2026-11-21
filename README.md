**Earn SOL by running a LastBuy Keeper. Receive rent exemption and unused lamports from migration funding for successfully executing permissionless migrations.**

LastBuy Keeper executes permissionless bonding curve migrations.

## Basic setup

Requires Rust, a funded Solana keypair, a v1-capable RPC and a Yellowstone gRPC endpoint.

```sh
# 1. Build the keeper.
cargo build --release --locked

# 2. Create the configuration files.
./target/release/keeper init

# 3. Check the configuration and connectivity.
./target/release/keeper check

# 4. Start the keeper.
./target/release/keeper run
```

After `keeper init`, configure these two files in the directory where you run the command:

- In `keeper.toml`, set `keypair_path` to the path of your funded Solana keypair. For the default Solana CLI keypair, the default value `~/.config/solana/id.json` is already correct. Example: `keypair_path = "/home/you/.config/solana/id.json"`.
- In the private `keeper.secrets.toml`, set `rpc_url` to your v1-capable Solana RPC URL and `grpc_url` to your Yellowstone gRPC endpoint URL. If your gRPC provider requires a token, set `grpc_x_token` there too.

For example, the minimum contents of `keeper.secrets.toml` are:

```toml
rpc_url = "https://YOUR_SOLANA_RPC_URL"
grpc_url = "https://YOUR_YELLOWSTONE_GRPC_URL"
# Add this only if your provider requires a token:
# grpc_x_token = "YOUR_YELLOWSTONE_TOKEN"
```

## Commands

`keeper check` validates the configuration and connectivity before you start with `keeper run`.

Use `keeper profit --config keeper.toml` to view realized net profit and failed transaction costs. Accounting uses finalized transaction balances and persists in the configured state directory.

Use `keeper reset-ledger --config keeper.toml` to permanently remove the active ledger. This discards its accounting, pending attempt history, and durable submission exclusions. The next `keeper run` initializes a fresh ledger.

## Advanced setup

Advanced settings can be configured in optional `keeper.advanced.toml`. Create it by copying `keeper.advanced.example.toml`; `keeper init` does not make it. `run` and `check` use it.
