use jx_crypto::{
    generate_keyfile, hash_password, load_keyfile, sm3_hash, sm4_gcm_open, sm4_gcm_seal,
    verify_password,
};
use std::os::unix::fs::PermissionsExt;

#[test]
fn sm3_password_roundtrip() {
    let salt = [7u8; 16];
    let h = hash_password("s3cret", &salt);
    assert!(verify_password("s3cret", &hex_encode(&salt), &h));
    assert!(!verify_password("wrong", &hex_encode(&salt), &h));
}

#[test]
fn sm3_hash_is_32_bytes() {
    let d = sm3_hash(b"MemoryDataBase");
    assert_eq!(d.len(), 32);
}

#[test]
fn sm4_file_roundtrip() {
    let key = [9u8; 32];
    let sealed = sm4_gcm_seal(&key, b"hello MemoryDataBase").unwrap();
    let opened = sm4_gcm_open(&key, &sealed).unwrap();
    assert_eq!(opened, b"hello MemoryDataBase");
    assert!(sm4_gcm_open(&[0u8; 32], &sealed).is_err());
}

#[test]
fn keyfile_permissions_and_load() {
    let dir = std::env::temp_dir().join(format!("jx-key-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("storage.key");
    generate_keyfile(&path).unwrap();
    let k = load_keyfile(&path).unwrap();
    assert_eq!(k.len(), 32);
    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn generate_keyfile_fails_if_exists() {
    let dir = std::env::temp_dir().join(format!("jx-key-ex-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("storage.key");
    generate_keyfile(&path).unwrap();
    let first = load_keyfile(&path).unwrap();
    let err = generate_keyfile(&path).unwrap_err();
    assert!(
        err.to_string().contains("already exists") || format!("{err:#}").contains("already exists"),
        "expected already-exists error, got: {err:#}"
    );
    let second = load_keyfile(&path).unwrap();
    assert_eq!(first, second);
    let _ = std::fs::remove_dir_all(&dir);
}

fn hex_encode(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
