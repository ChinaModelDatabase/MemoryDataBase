use anyhow::{Context, Result};
use base64::Engine;
use std::path::Path;

/// TLCP transport configuration: dual certificates (sign + enc) plus CA bundle.
///
/// PEM bytes are stored as-is; conversion to DER / SM2 key pairs happens in
/// [`TlcpAcceptor::from_config`].
#[derive(Debug, Clone)]
pub struct TlcpConfig {
    pub sign_cert_pem: Vec<u8>,
    pub sign_key_pem: Vec<u8>,
    pub enc_cert_pem: Vec<u8>,
    pub enc_key_pem: Vec<u8>,
    pub ca_pem: Vec<u8>,
}

impl TlcpConfig {
    /// Load a full TLCP config from PEM files on disk.
    pub fn from_pem_files(
        sign_cert: impl AsRef<Path>,
        sign_key: impl AsRef<Path>,
        enc_cert: impl AsRef<Path>,
        enc_key: impl AsRef<Path>,
        ca: impl AsRef<Path>,
    ) -> Result<Self> {
        Ok(Self {
            sign_cert_pem: std::fs::read(sign_cert.as_ref()).context("read sign cert")?,
            sign_key_pem: std::fs::read(sign_key.as_ref()).context("read sign key")?,
            enc_cert_pem: std::fs::read(enc_cert.as_ref()).context("read enc cert")?,
            enc_key_pem: std::fs::read(enc_key.as_ref()).context("read enc key")?,
            ca_pem: std::fs::read(ca.as_ref()).context("read ca")?,
        })
    }
}

/// TLCP backend selected at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlcpBackend {
    /// Pure-Rust gm-tlcp (compiled in as a normal dependency).
    PureRust,
    /// GmSSL C-library FFI (not wired in MVP).
    GmsslFfi,
    /// No usable TLCP backend.
    Unsupported,
}

/// Detect which TLCP backend is available in this build.
pub fn detect_backend() -> TlcpBackend {
    // gm-tlcp is a hard dependency on the PureRust path.
    TlcpBackend::PureRust
}

/// Returns true when the config carries a full dual-cert + CA PEM set.
///
/// Stricter than a two-field check: Task 12 wiring requires all five inputs.
pub fn tlcp_enabled_and_ready(cfg: &TlcpConfig) -> bool {
    !cfg.sign_cert_pem.is_empty()
        && !cfg.sign_key_pem.is_empty()
        && !cfg.enc_cert_pem.is_empty()
        && !cfg.enc_key_pem.is_empty()
        && !cfg.ca_pem.is_empty()
}

/// Decode the first PEM CERTIFICATE block to DER.
///
/// Public so tests (and future callers) share one implementation instead of
/// re-implementing PEM parsing.
pub fn pem_cert_to_der(pem: &[u8], label: &str) -> Result<Vec<u8>> {
    let text = std::str::from_utf8(pem).with_context(|| format!("{label} pem utf-8"))?;
    let begin = "-----BEGIN CERTIFICATE-----";
    let end = "-----END CERTIFICATE-----";
    let start = text
        .find(begin)
        .with_context(|| format!("{label}: missing {begin}"))?;
    let stop = text[start..]
        .find(end)
        .with_context(|| format!("{label}: missing {end}"))?
        + start;
    let body = &text[start + begin.len()..stop];
    let b64: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .with_context(|| format!("{label}: base64 decode cert"))
}

/// Parse an unencrypted SM2 private key PEM (SEC1 or PKCS#8).
///
/// Password-encrypted keys are rejected: MVP accepts only unencrypted keys.
/// Encrypted-key support is deferred; the error message says so.
fn parse_sm2_key(pem: &[u8], label: &str) -> Result<gm_crypto::sm2::Sm2KeyPair> {
    let text = std::str::from_utf8(pem).with_context(|| format!("{label} pem utf-8"))?;
    // Prefer the raw PEM; fall back to stripping public-key blocks that
    // GmSSL concatenates into the same file.
    let candidates = [text, extract_private_key_pem(text)];
    let mut last_err = String::new();
    for cand in candidates {
        if cand.trim().is_empty() {
            continue;
        }
        if cand.contains("ENCRYPTED PRIVATE KEY") {
            anyhow::bail!(
                "{label}: encrypted SM2 key PEM is unsupported in MVP; \
                 provide an unencrypted PEM key"
            );
        }
        match gm_crypto::sm2::Sm2KeyPair::from_private_key_pem(cand) {
            Ok(kp) => return Ok(kp),
            Err(e) => last_err = e.to_string(),
        }
    }
    Err(anyhow::anyhow!("{label}: parse SM2 key: {last_err}"))
}

fn extract_private_key_pem(text: &str) -> &str {
    let begin = "-----BEGIN ";
    let end = "-----END ";
    let mut search = 0usize;
    while let Some(bs) = text[search..].find(begin) {
        let start = search + bs;
        let Some(rel_end) = text[start..].find(end) else {
            break;
        };
        let end_tag_start = start + rel_end;
        let Some(rel_close) = text[end_tag_start..].find("-----") else {
            break;
        };
        let mut stop = end_tag_start + rel_close + 5;
        if stop < text.len() && text.as_bytes()[stop] == b'\n' {
            stop += 1;
        }
        let block = &text[start..stop];
        if block.contains("PRIVATE KEY") {
            return block;
        }
        search = stop;
    }
    text
}

/// TLCP server acceptor wrapping gm-tlcp's dual-certificate acceptor.
pub struct TlcpAcceptor {
    inner: gm_tlcp::TlcpAcceptor,
}

impl TlcpAcceptor {
    /// Build a production dual-cert acceptor from PEM config.
    ///
    /// CA PEM is retained on [`TlcpConfig`] for future client-auth chain
    /// validation (`with_client_ca_chain`); it is not required by the
    /// current acceptor handshake path.
    pub fn from_config(cfg: &TlcpConfig) -> Result<Self> {
        if !tlcp_enabled_and_ready(cfg) {
            anyhow::bail!("TLCP config incomplete: need sign/enc cert+key and ca PEMs");
        }
        let sign_der = pem_cert_to_der(&cfg.sign_cert_pem, "sign cert")?;
        let enc_der = pem_cert_to_der(&cfg.enc_cert_pem, "enc cert")?;
        let sign_key = parse_sm2_key(&cfg.sign_key_pem, "sign key")?;
        let enc_key = parse_sm2_key(&cfg.enc_key_pem, "enc key")?;
        let inner =
            gm_tlcp::TlcpAcceptor::new().with_dual_certs(sign_der, enc_der, sign_key, enc_key);
        Ok(Self { inner })
    }

    /// Perform a production TLCP handshake over the given TCP stream.
    pub async fn accept(&self, stream: tokio::net::TcpStream) -> Result<TlcpStream, anyhow::Error> {
        let s = self
            .inner
            .accept_with_certs(stream)
            .await
            .map_err(|e| anyhow::anyhow!("TLCP accept handshake failed: {e}"))?;
        Ok(TlcpStream { inner: s })
    }
}

/// Encrypted TLCP stream (AsyncRead + AsyncWrite + Unpin).
pub struct TlcpStream {
    inner: gm_tlcp::TlcpStream<tokio::net::TcpStream>,
}

impl TlcpStream {
    /// Read one decrypted application-data record.
    pub async fn read_application_data(&mut self) -> Result<Vec<u8>, anyhow::Error> {
        self.inner
            .read_application_data()
            .await
            .map_err(|e| anyhow::anyhow!("TLCP read application data: {e}"))
    }

    /// Write one encrypted application-data record.
    pub async fn write_application_data(&mut self, plaintext: &[u8]) -> Result<(), anyhow::Error> {
        self.inner
            .write_application_data(plaintext)
            .await
            .map_err(|e| anyhow::anyhow!("TLCP write application data: {e}"))
    }
}

impl tokio::io::AsyncRead for TlcpStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for TlcpStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

impl Unpin for TlcpStream {}
