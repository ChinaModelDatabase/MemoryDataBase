use jx_server::config::{load_config, TlsMode};
use std::io::Write;

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
