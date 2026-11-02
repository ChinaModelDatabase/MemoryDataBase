use jx_server::config::{load_config, Role};
use std::io::Write;

fn write_conf(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join("jx.conf");
    std::fs::write(&path, body).unwrap();
    path
}

#[test]
fn parse_role_and_replicaof() {
    let dir = std::env::temp_dir().join(format!("jx-repl-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("jx.conf");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "bind 127.0.0.1").unwrap();
    writeln!(f, "port 6379").unwrap();
    writeln!(f, "role replica").unwrap();
    writeln!(f, "replicaof 127.0.0.1 6380").unwrap();
    writeln!(f, "repl-port 6381").unwrap();
    writeln!(f, "backlog-size 1mb").unwrap();
    writeln!(f, "repl-timeout 30").unwrap();
    writeln!(f, "masterauth sm3hash").unwrap();
    let cfg = load_config(&path).unwrap();
    assert_eq!(cfg.role, Role::Replica);
    assert_eq!(cfg.replicaof, Some(("127.0.0.1".to_string(), 6380)));
    assert_eq!(cfg.repl_port, 6381);
    assert_eq!(cfg.backlog_size, 1024 * 1024);
    assert_eq!(cfg.repl_timeout_secs, 30);
    assert_eq!(cfg.masterauth.as_deref(), Some("sm3hash"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn repl_defaults_when_unset() {
    let dir = std::env::temp_dir().join(format!("jx-repl-defaults-{}", std::process::id()));
    let path = write_conf(&dir, "bind 127.0.0.1\nport 6379\n");
    let cfg = load_config(&path).unwrap();
    assert_eq!(cfg.role, Role::Standalone);
    assert_eq!(cfg.replicaof, None);
    assert_eq!(cfg.repl_port, 6380);
    assert_eq!(cfg.backlog_size, 64 * 1024 * 1024);
    assert_eq!(cfg.repl_timeout_secs, 60);
    assert_eq!(cfg.masterauth, None);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn reject_bad_role_and_replicaof() {
    let dir = std::env::temp_dir().join(format!("jx-repl-bad-{}", std::process::id()));
    let bad_role = write_conf(&dir, "port 6379\nrole slave\n");
    assert!(load_config(&bad_role).is_err());
    let bad_replicaof = write_conf(&dir, "port 6379\nrole replica\nreplicaof 127.0.0.1\n");
    assert!(load_config(&bad_replicaof).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}
