# workflow

Host-neutral workflow engine shared by CulebraLuxe-web (RE host) and Forge (SDLC host).

- `crates/workflow-core` — pure engine/kernel: definitions, expressions, engine loop, store traits,
  types, status, ids, hashing, memory store, XML parser. **Zero dependencies.**
- Hosts provide their own ports, node handlers, XML definitions, persistence adapters and migrations.

No database, model, app or Forge code lives here. The Neon/Postgres adapter (`workflow-neon`) remains
in the app repository until its `db` coupling is removed.

## Versioning

Tags `vMAJOR.MINOR.PATCH`. Hosts pin a tag; `Cargo.lock` pins the commit. Bump PRs in both hosts must
run their smoke/conformance suites.
