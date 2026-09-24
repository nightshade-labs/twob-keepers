# Program interfaces

- `twob_anchor.json`: the bookkeeper's current devnet deployment,
  `CCAdkkosRFpzrb1BAWHnrzVGHMg4nNmurFCQefn7JtLX`. Copied from the deployment's
  generated `target/idl/twob_anchor.json` on 2026-09-24, based on
  `nightshade-labs/twob-anchor` v1 commit `94824f8` plus the new deployment ID.
- `twob_anchor_legacy.json`: the unchanged IDL from keepers commit `7ad7800`,
  targeting `CCAd78ZgUBAFNQmCCD5z4oGuFzb8uXLw5kfnBcRvDw16`. The event and trade
  keepers remain pinned to this interface pending their separate migrations.

The legacy program was closed on devnet. Those services cannot operate against
the new deployment by changing RPC URLs alone. In particular, v1 events identify
markets by address, while the existing database and read API use numeric IDs.
Do not replace the legacy IDL with the current one without migrating its callers.

Program addresses are compiled from these IDLs. Rebuild the affected binary
after updating an IDL. The current bookkeeper uses 7-slot boundaries and
30-entry intervals; the legacy trade keeper retains its 20-entry intervals and
reads the slot interval from its legacy Market account.
