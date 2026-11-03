use jx_server::config::{load_config, TlcpDistidPolicy, TlsMode};
use std::io::Write;
use std::path::PathBuf;

#[test]
fn load_example_config() {
    let dir = std::env::temp_dir().join(format!("jx-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("jx.conf");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "bind 127.0.0.1").unwrap();
    writeln!(f, "port 6379").unwrap();
    writeln!(f, "tls-mode plain").unwrap();
    writeln!(f, "shards 4").unwrap();
    writeln!(f, "maxmemory 256mb").unwrap();
    writeln!(f, "appendonly yes").unwrap();
    writeln!(f, "appendfsync everysec").unwrap();
    writeln!(f, "dir {}", dir.display()).unwrap();
    let cfg = load_config(&path).unwrap();
    assert_eq!(cfg.port, 6379);
    assert_eq!(cfg.tls_mode, TlsMode::Plain);
    assert!(cfg.appendonly);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn validate_rejects_bad_tls_mode() {
    let dir = std::env::temp_dir().join(format!("jx-cfg-bad-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("jx.conf");
    std::fs::write(&path, "tls-mode nope\nport 1\nbind 127.0.0.1\n").unwrap();
    assert!(load_config(&path).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tlcp_distid_policy_defaults_to_strict_and_parses_permissive() {
    let dir = std::env::temp_dir().join(format!("jx-cfg-tlcp-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let fx = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../jx-crypto/tests/fixtures/tlcp");
    let write = |body: &str| {
        let path = dir.join("jx.conf");
        std::fs::write(&path, body).unwrap();
        path
    };
    let certs = format!(
        "tlcp-sign-cert {}\ntlcp-sign-key {}\ntlcp-enc-cert {}\ntlcp-enc-key {}\ntlcp-ca {}\n",
        fx.join("sign.crt").display(),
        fx.join("sign.key").display(),
        fx.join("enc.crt").display(),
        fx.join("enc.key").display(),
        fx.join("ca.crt").display(),
    );

    let strict_path = write(&format!("bind 127.0.0.1\nport 1\ntls-mode tlcp\n{certs}"));
    let cfg = load_config(&strict_path).unwrap();
    assert_eq!(
        cfg.tlcp.as_ref().unwrap().distid_policy,
        TlcpDistidPolicy::Strict,
        "trust-anchor verification must be strict by default"
    );

    let permissive_path = write(&format!(
        "bind 127.0.0.1\nport 1\ntls-mode tlcp\n{certs}tlcp-distid-policy permissive\n"
    ));
    let cfg = load_config(&permissive_path).unwrap();
    assert_eq!(
        cfg.tlcp.as_ref().unwrap().distid_policy,
        TlcpDistidPolicy::PermissiveOpenSsl
    );

    let bad_path = write(&format!(
        "bind 127.0.0.1\nport 1\ntls-mode tlcp\n{certs}tlcp-distid-policy loose\n"
    ));
    assert!(
        load_config(&bad_path).is_err(),
        "unknown distid policy must be rejected"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
