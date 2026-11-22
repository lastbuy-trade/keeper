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

## Docker

Build the image with Docker. The image runs as an unprivileged user and defaults to `keeper --help`.

```sh
docker build -t lastbuy-keeper .
mkdir -p config state
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/config:/config" lastbuy-keeper init
```

Edit `config/keeper.toml` and `config/keeper.secrets.toml` with your endpoint settings. The image reads the keypair from `/run/keeper/id.json` and stores its ledger in `/state`. Keep the funded keypair outside the image; this mounts it read-only and persists the ledger in `state/`:

```sh
docker run --rm \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=$PWD/config,dst=/config,readonly" \
  --mount "type=bind,src=$PWD/state,dst=/state" \
  --mount "type=bind,src=$HOME/.config/solana/id.json,dst=/run/keeper/id.json,readonly" \
  lastbuy-keeper run
```

Use the same command with `check` or `profit` in place of `run` to check connectivity or view accounting. Running with your host UID/GID lets the process read private host files and keeps ledger files owned by your host user. Credentials stay in the mounted private secrets file, not image layers or command arguments.

If your validator runs on the Docker Desktop host, replace `localhost` with `host.docker.internal` in the endpoint URLs. `keeper check` was verified with `http://host.docker.internal:8899` for JSON-RPC and `http://host.docker.internal:10000` for Yellowstone gRPC.
