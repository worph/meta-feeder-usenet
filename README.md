# meta-feeder-usenet

Usenet feeder sidecar for the MetaMesh gateway tier. Bridges a Usenet header
scan **we run ourselves** (an [nntmux](https://github.com/NNTmux/newznab-tmux)
sidecar) into the MetaMesh network.

## Why this exists

`meta-feeder-indexer` proxies an **external** Newznab indexer and mints an
`nzb-release` (`0x1005`) cid that embeds *that indexer's host*. Only a peer
holding that indexer's credential can ever redeem it — so the metadata half of
Usenet is neither self-owned nor portable.

Scanning Usenet ourselves gives us the article Message-IDs for free at collation
time (no grab, no scrape). That lets this feeder mint a **portable**
`nzb-posting` (`0x1003`) cid — a digest over the Message-ID set, embedding no
host — which **any peer with a plain NNTP provider can redeem, with no indexer
credential anywhere in the path**.

This feeder **adds** a Usenet metadata source alongside `meta-feeder-indexer`.
It replaces nothing; a gateway query fans out to both.

It also **redeems external releases.** The torznab feeder mints `nzb-release`
(`0x1005`) locators from Prowlarr/Newznab search hits; when a viewer plays one,
the gateway asks this feeder (`POST /compute` with the locator cid) to grab its
`.nzb` with the key configured here for that indexer host. The Newznab keys live
on this feeder — meta-share keeps only the NNTP provider. See
[Redeeming nzb-release locators](#redeeming-nzb-release-locators).

Design record:
`meta-gateway/docs/others/self-hosted-usenet-indexer-study.md`.

## Shape

```
[nntmux sidecar: PHP + MariaDB + Redis]     [usenet-feeder: Rust, FeederPlugin]
  header-scan groups (few conns)   ──►  reads `releases` (searchname/size/guid)
  collate → releases               ──►  gunzips <nzb_root>/<c>/<guid>.nzb.gz
  writes <guid>.nzb.gz             ──►  mints 0x1003 from the Message-ID set
  (CBP purged; the gz is the only  ──►  emits the record + a relative
   durable home of the msg-ids)          `manifest_url` onto its own /blob route
                                                    │
                                                    ▼
   gateway core: seeds the .nzb as an ORDINARY sha2-256 IPFS cid, publishes it
   as a fileType=nzb meta-core record, rewrites the field to that cid
   (the same path posters take — `remote_feeder.rs` SEEDABLE_FIELDS)
                                                    │
                                                    ▼
   meta-share: resolve `manifest` cid (blockstore / bitswap / ANY holder) →
   NzbManifest::from_nzb_xml → BODY-fetch each Message-ID from ANY plain NNTP
   provider → materialise → play
```

The sidecar is **persistent cache, cheap to lose** — everything in it is
re-derivable by re-scanning. Same pattern as the Tribler sidecar: stateful
upstream, but the gateway still sees a stateless HTTP feeder.

Like every feeder this one is **meta-core-free and blockstore-free**: it finds
records and serves bytes; the core owns hashing-into-the-blockstore, the
meta-core store-back, and the libp2p wire (gateway invariant 10).

## Configuration

Operator config lives on the **feeder**, not the gateway (invariant 12). Set it
on the plugin's config page in the gateway dashboard; the env vars below are a
**first-boot seed only** — the persisted `config.json` wins on the next restart,
and there is no hot reload.

| Field | Env seed | What |
|---|---|---|
| `db_url` | `NNTMUX_DB_URL` | MariaDB DSN of nntmux's catalog, e.g. `mysql://nntmux:secret@nntmux-db:3306/nntmux`. Blank → self-scan search is off. |
| `nzb_root` | `NNTMUX_NZB_PATH` | nntmux's NZB directory **as mounted into this container**. Must be the same volume nntmux's `PATH_TO_NZBS` points at, or every release resolves to `NotFound`. |
| `indexers` | — (config page only) | `[{host, api_key}]` Newznab keys for redeeming `nzb-release` locators. `host` is the bare authority (`api.nzbgeek.info` — no scheme, no `/api`). |

Both halves are optional; with neither configured the plugin reports Degraded.
A grab-only deployment (keys, no nntmux) is valid.

Infra env: `META_FEEDER_HTTP_LISTEN` (default `0.0.0.0:8080`),
`META_FEEDER_STATE_DIR` (default `/data/meta-feeder`), `RUST_LOG`.

Scan policy — which groups, backfill depth, scan interval, connection budget —
is the **gateway maintainer's** call and is configured on the nntmux sidecar.
Keep the connection budget conservative: the scanner and playback draw on the
same provider slots, so a greedy value does not merely over-scan, it stalls
playback mid-stream.

## Redeeming nzb-release locators

`POST /compute {upstream_id: "usenet", record_id: <0x1005 cid>}`:

1. Decode `{api_base, id}` from the cid; find the key for its host. No key →
   `404` (not ours — the gateway tries another claimer, nothing is spent).
2. `GET https://{api_base}/api?t=get&id=…&apikey=…` with a named User-Agent
   (nzbgeek answers `109 Invalid User Agent` without one). **This spends the
   indexer's daily download quota** — the gateway only calls it on a real play,
   and never again for a release whose `manifest` it already stored.
3. Strip the `.nzb` `<head>` except `<meta type="password">` (the file is seeded
   to the whole mesh; the password is needed to unpack a protected RAR set).
4. Mint the portable `nzb-posting` (`0x1003`) cid from its Message-IDs.
5. Answer ONE `sha2_256` outcome: the `.nzb` bytes (`file_extension: nzb`) and a
   record carrying `cids/<nzb-posting>` + `segmentCount`. The gateway stores the
   file under `/files/plugin/meta-feeder-usenet/`, writes its cid onto the release
   record as `manifest`, and merges the posting cid — after which any peer with a
   plain NNTP provider can play the release.

Errors: an indexer credential/account refusal (`100`–`102`, `910`) or an HTML
page → `422`; request/download limit (`429`, `500`, `501`, or HTTP 429) → `429`;
no such item (`300`) → `404`.

`GET /redeems` and `/manifest` advertise `{codec: nzb-release, field: manifest,
hosts: [...]}` for the hosts that have a key; nothing without keys.

## Build

```bash
cargo build --release --bin usenet-feeder
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

docker build -f feeder-plugin/usenet-feeder/Dockerfile -t ghcr.io/worph/meta-feeder-usenet:dev .
```

`meta-feeder-sdk` is a git dependency pinned by tag (`v1.3.0`; this repo used to
vendor a copy). Until that tag is pushed, cargo cannot resolve it — not even
under a `[patch]` override. To verify against a local SDK checkout, build a
scratch copy with the dependency swapped for a path (never commit that form —
this repo is the Docker build context):

```bash
SCRATCH=/d/workspace/tmp-claude/usenet-feeder; SDK=/d/workspace/MetaMesh/meta-root-v2/packages/plugins/meta-feeder-sdk
rm -rf $SCRATCH && mkdir -p $SCRATCH && tar cf - --exclude=./target --exclude=./.git . | (cd $SCRATCH && tar xf -)
sed -i "s#meta-feeder-sdk = { git = .*#meta-feeder-sdk = { path = \"$SDK\" }#" $SCRATCH/feeder-plugin/*/Cargo.toml
docker run --rm -e RUSTUP_TOOLCHAIN=1.89.0 -e CARGO_TARGET_DIR=/target -v /d/workspace:/d/workspace \
  -v feeder-cargo-registry:/usr/local/cargo/registry -v usenet-feeder-target:/target -w $SCRATCH \
  rust:1.89 sh -c 'apt-get install -y -qq pkg-config libssl-dev >/dev/null 2>&1; cargo test'
```

After the tag is pushed, run `cargo update -p meta-feeder-sdk` here so
`Cargo.lock` stops pointing at the removed vendored path.

`compute_nzb_posting_cid`'s normalisation rule is the dedup contract and is
mirrored on the meta-share side — change them together.

## Gotchas

- **An empty catalog is the visible symptom of both nntmux silent-zero traps**:
  an empty `PATH_TO_NZBS` (set-but-empty beats the default, so `createNZBs()`
  aborts and you get 0 releases with no error), and seeded groups left
  `active=0`. The plugin logs a loud warning when `releases` is empty at boot —
  believe it.
- **The Message-IDs are not in the database.** nntmux purges
  collections/binaries/parts right after writing each NZB, so the on-disk
  `.nzb.gz` is the only durable home. A wrong `nzb_root` therefore fails at
  `compute_outcomes`, not at query time.
