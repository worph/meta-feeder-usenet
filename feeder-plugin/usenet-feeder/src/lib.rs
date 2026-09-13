//! `usenet-feeder` library surface. The binary (`main.rs`) wraps the plugin in
//! [`meta_feeder_sdk::serve_feeders`].
//!
//! This feeder owns the **metadata half** of Usenet: it reads an nntmux header-
//! scan catalog we run ourselves and mints a portable `nzb-posting` (`0x1003`)
//! cid from each release's article Message-IDs. Unlike `meta-feeder-indexer`'s
//! `0x1005` locator, that cid embeds no indexer host, so any peer with a plain
//! NNTP provider can redeem it — no indexer credential anywhere in the path.
//!
//! It ADDS a source alongside meta-feeder-indexer; it replaces nothing.
//!
//! It also **redeems** external Newznab releases: an `nzb-release` (`0x1005`)
//! cid minted by meta-feeder-torznab is grabbed here, with this feeder's
//! per-host indexer keys, when the gateway calls `/compute` on a real play
//! ([`newznab`]). Indexer keys live on this feeder, not on meta-share.
//!
//! Like every feeder it is meta-core-free and blockstore-free: it finds records
//! and serves bytes, and the gateway core owns hashing-into-the-blockstore, the
//! meta-core store-back, and the libp2p wire (gateway invariant 10).
//!
//! See `meta-gateway/docs/others/self-hosted-usenet-indexer-study.md`.

/// Read-only access to the nntmux sidecar: its MariaDB release catalog and the
/// on-disk gzip NZBs that are the *only* durable home of the Message-IDs.
pub mod nntmux {
    pub mod db;
    pub mod nzb;
}

/// The `t=get` grab that redeems an external `nzb-release` locator.
pub mod newznab;

/// The `usenet` upstream itself.
pub mod usenet;
