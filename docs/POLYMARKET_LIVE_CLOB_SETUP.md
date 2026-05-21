# Polymarket Live CLOB Setup

This repo still keeps Polymarket live trading disabled. Use this note only to prepare and inspect credentials locally; do not wire live order posting into the paper workflow without a separate risk review.

## What Is Needed

Real CLOB trading needs separate pieces:

- `PRIVATE_KEY`: owner signer used locally to create/derive CLOB credentials and sign orders.
- `CLOB_API_KEY`, `CLOB_SECRET`, `CLOB_PASS_PHRASE`: L2 CLOB credentials created from the private key.
- `DEPOSIT_WALLET_ADDRESS`: funder address for the new deposit-wallet flow.
- `BUILDER_API_KEY`, `BUILDER_SECRET`, `BUILDER_PASS_PHRASE`: builder credentials used by the Python relayer client for deposit wallet deployment and wallet batches.
- Funds: pUSD must be held by the deposit wallet, not just the owner EOA.
- Allowance: approvals must be made from the deposit wallet by relayer wallet batch, then the CLOB balance/allowance cache must be synced.

`RELAYER_API_KEY` and `RELAYER_API_KEY_ADDRESS` authenticate relayer `/submit` calls. They do not replace CLOB L2 credentials and do not sign orders.

## One-Time Local Setup

Install the live-only SDKs into your local environment:

```powershell
pip install py-clob-client-v2 py-builder-relayer-client
```

Put secrets in an ignored file such as `secrets/polymarket_live.env`:

```text
PRIVATE_KEY=0x...
CHAIN_ID=137
CLOB_API_URL=https://clob.polymarket.com
RELAYER_URL=https://relayer-v2.polymarket.com

# If using the Python relayer client:
BUILDER_API_KEY=...
BUILDER_SECRET=...
BUILDER_PASS_PHRASE=...

# Optional raw relayer auth; not CLOB auth:
RELAYER_API_KEY=...
RELAYER_API_KEY_ADDRESS=0x...
```

The setup script accepts uppercase `KEY=value` and `KEY: value` lines. It also maps lowercase `address:` and `key:` to relayer auth only when their values look like the current `keys.txt` relayer fields; other lowercase fields are ignored to avoid confusing notes or URLs with secrets.

Check the local environment:

```powershell
python scripts\polymarket_clob_setup.py doctor --env-file secrets\polymarket_live.env
```

Verify relayer auth without submitting transactions:

```powershell
python scripts\polymarket_clob_setup.py check-relayer-auth --env-file secrets\polymarket_live.env
```

Create or derive CLOB L2 credentials:

```powershell
python scripts\polymarket_clob_setup.py derive-clob-creds --env-file secrets\polymarket_live.env
```

The script writes full credentials to `secrets/polymarket_clob.env` by default and prints only masked values.

Derive the expected deposit wallet:

```powershell
python scripts\polymarket_clob_setup.py derive-deposit-wallet --env-file secrets\polymarket_live.env --env-file secrets\polymarket_clob.env
```

Deploying a deposit wallet is live relayer activity, so it requires an explicit flag:

```powershell
python scripts\polymarket_clob_setup.py deploy-deposit-wallet --env-file secrets\polymarket_live.env --env-file secrets\polymarket_clob.env --yes --wait
```

After the deposit wallet is deployed, fund it with pUSD. Funds sitting on the owner EOA do not count as buying power for `POLY_1271` orders.

After funding and approving contracts from the deposit wallet, sync the CLOB cache:

```powershell
python scripts\polymarket_clob_setup.py sync-balance-allowance --env-file secrets\polymarket_live.env --env-file secrets\polymarket_clob.env
```

For selling conditional tokens, sync that token too:

```powershell
python scripts\polymarket_clob_setup.py sync-balance-allowance --env-file secrets\polymarket_live.env --env-file secrets\polymarket_clob.env --asset-type CONDITIONAL --token-id <TOKEN_ID>
```

## Important Guardrails

- Do not paste private keys into chat.
- Do not commit files under `secrets/`, `.env`, or `keys.txt`.
- Do not use relayer credentials as CLOB credentials; the auth systems are separate.
- Do not trade from restricted jurisdictions.
- Keep live order posting separate from this setup script until the repo has manual approval, max notional limits, heartbeat/cancel-all, audit logs, and a kill switch for Polymarket live orders.
