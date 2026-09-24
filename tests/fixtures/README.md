# Devnet account snapshot

`devnet-sol-usdc.json` contains public Market and Bookkeeping account bytes read
from devnet on 2026-09-24 with confirmed commitment. The response's context slot,
program ID, account addresses, and decoded expected last-update slot are saved
alongside the base64 data. No signing keys or credentials are included.

The bookkeeper regression test deserializes these bytes with the bundled IDL
and checks the canonical market/bookkeeping PDAs, stored bumps, market linkage,
and last-update slot. Using a real account snapshot catches layout changes that
a generated serialize/deserialize round-trip would miss.
