//! SFTP client and server over any `AsyncRead + AsyncWrite` stream (here a russh
//! channel), with the `check-file` extension.
//!
//! Contains third-party code, modified from its original form; Apache-2.0 (see LICENSE).
#![allow(dead_code, unused_imports)] // not all items are used by every consumer

mod buf;
pub mod client;
pub mod de;
mod error;
pub mod extensions;
pub mod protocol;
pub mod ser;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;
mod utils;
