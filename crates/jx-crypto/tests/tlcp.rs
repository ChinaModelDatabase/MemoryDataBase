use jx_crypto::{
    detect_backend, pem_cert_to_der, tlcp_enabled_and_ready, TlcpAcceptor, TlcpBackend, TlcpConfig,
    TlcpDistidPolicy,
};
use std::path::PathBuf;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tlcp")
}

fn load_fixture_config() -> TlcpConfig {
    let dir = fixture_dir();
    TlcpConfig::from_pem_files(
        dir.join("sign.crt"),
        dir.join("sign.key"),
        dir.join("enc.crt"),
        dir.join("enc.key"),
        dir.join("ca.crt"),
    )
    .expect("load tlcp pems")
}

#[test]
fn tlcp_config_loads_pems() {
    let cfg = load_fixture_config();
    assert!(!cfg.sign_cert_pem.is_empty());
    assert!(!cfg.sign_key_pem.is_empty());
    assert!(!cfg.enc_cert_pem.is_empty());
    assert!(!cfg.enc_key_pem.is_empty());
    assert!(!cfg.ca_pem.is_empty());
}

#[test]
fn tlcp_enabled_and_ready_true() {
    let cfg = load_fixture_config();
    assert!(tlcp_enabled_and_ready(&cfg));
}

#[test]
fn tlcp_enabled_and_ready_false() {
    let empty = TlcpConfig {
        sign_cert_pem: Vec::new(),
        sign_key_pem: b"key".to_vec(),
        enc_cert_pem: b"cert".to_vec(),
        enc_key_pem: b"key".to_vec(),
        ca_pem: b"ca".to_vec(),
    };
    assert!(!tlcp_enabled_and_ready(&empty));

    let partial = TlcpConfig {
        sign_cert_pem: b"cert".to_vec(),
        sign_key_pem: Vec::new(),
        enc_cert_pem: b"cert".to_vec(),
        enc_key_pem: b"key".to_vec(),
        ca_pem: b"ca".to_vec(),
    };
    assert!(!tlcp_enabled_and_ready(&partial));
}

/// The acceptor is configured with `tlcp-ca` trust anchors, so a client whose
/// certificate chain is not signed by that CA must fail the handshake.
#[tokio::test]
async fn tlcp_loopback_rejects_untrusted_client_cert() {
    let cfg = load_fixture_config();
    let acceptor = TlcpAcceptor::from_config_with_policy(&cfg, TlcpDistidPolicy::PermissiveOpenSsl)
        .expect("acceptor from fixtures");

    let dir = fixture_dir();
    let read = |p: &str| std::fs::read(dir.join(p)).unwrap_or_else(|e| panic!("read {p}: {e}"));
    let rogue_chain = vec![
        pem_cert_to_der(&read("rogue-sign.crt"), "rogue sign").expect("rogue sign der"),
        pem_cert_to_der(&read("rogue-enc.crt"), "rogue enc").expect("rogue enc der"),
        pem_cert_to_der(&read("rogue-ca.crt"), "rogue ca").expect("rogue ca der"),
    ];
    let rogue_sign_key =
        std::fs::read_to_string(dir.join("rogue-sign.key")).expect("read rogue sign key");
    let rogue_enc_key =
        std::fs::read_to_string(dir.join("rogue-enc.key")).expect("read rogue enc key");

    let sign_key_text = std::str::from_utf8(&cfg.sign_key_pem).expect("sign key utf-8");
    let sign_kp = gm_crypto::sm2::Sm2KeyPair::from_private_key_pem(sign_key_text)
        .expect("parse sign key for pubkey");
    let sign_pub = sign_kp.public_key_bytes_uncompressed();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept tcp");
        acceptor.accept(tcp).await.map(|_| ())
    });

    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let connector = gm_tlcp::TlcpConnector::new()
        .with_server_sign_key(sign_pub, "1234567812345678".to_string())
        .with_client_certs(rogue_chain, rogue_sign_key, Some(rogue_enc_key), None);
    assert!(
        connector.connect_with_certs(tcp).await.is_err(),
        "client must not complete the handshake with an untrusted chain"
    );

    let server_result = server.await.expect("server task");
    let Err(e) = server_result else {
        panic!("server must reject a client cert outside the configured CA");
    };
    let msg = format!("{e:#}").to_lowercase();
    assert!(
        msg.contains("trust") || msg.contains("anchor") || msg.contains("cert"),
        "rejection must report a certificate trust failure, got: {msg}"
    );
}

#[test]
fn detect_backend_is_pure_rust() {
    assert_eq!(detect_backend(), TlcpBackend::PureRust);
}

#[test]
fn tlcp_acceptor_from_config_builds() {
    let cfg = load_fixture_config();
    TlcpAcceptor::from_config(&cfg).expect("build TlcpAcceptor from fixture PEMs");
}

#[tokio::test]
async fn tlcp_loopback_handshake() {
    let cfg = load_fixture_config();
    let acceptor = TlcpAcceptor::from_config_with_policy(&cfg, TlcpDistidPolicy::PermissiveOpenSsl)
        .expect("TlcpAcceptor must succeed with fixture certs (OpenSSL-issued)");

    // gm-tlcp 0.7 requires: (1) client pin of server sign pubkey for SKE,
    // (2) client dual certs because the acceptor sends CertificateRequest.
    let sign_key_text = std::str::from_utf8(&cfg.sign_key_pem).expect("sign key utf-8");
    let sign_kp = gm_crypto::sm2::Sm2KeyPair::from_private_key_pem(sign_key_text)
        .expect("parse sign key for pubkey");
    let sign_pub = sign_kp.public_key_bytes_uncompressed();

    let client_chain = vec![
        pem_cert_to_der(&cfg.sign_cert_pem, "sign cert").expect("sign cert der"),
        pem_cert_to_der(&cfg.enc_cert_pem, "enc cert").expect("enc cert der"),
        pem_cert_to_der(&cfg.ca_pem, "ca cert").expect("ca cert der"),
    ];
    let sign_key_pem = String::from_utf8(cfg.sign_key_pem.clone()).expect("sign key utf-8");
    let enc_key_pem = String::from_utf8(cfg.enc_key_pem.clone()).expect("enc key utf-8");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.expect("accept tcp");
        let mut s = acceptor.accept(tcp).await.expect("tlcp accept");
        let msg = s.read_application_data().await.expect("server read");
        s.write_application_data(&msg).await.expect("server echo");
        msg
    });

    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let connector = gm_tlcp::TlcpConnector::new()
        .with_server_sign_key(sign_pub, "1234567812345678".to_string())
        .with_client_certs(client_chain, sign_key_pem, Some(enc_key_pem), None);
    let mut client = connector
        .connect_with_certs(tcp)
        .await
        .expect("tlcp connect");

    let payload = b"hello-tlcp-loopback";
    client
        .write_application_data(payload)
        .await
        .expect("client write");
    let echoed = client.read_application_data().await.expect("client read");
    assert_eq!(echoed, payload);

    let server_msg = server.await.expect("server task");
    assert_eq!(server_msg, payload);
}
