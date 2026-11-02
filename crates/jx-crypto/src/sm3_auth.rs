use sm3::{Digest, Sm3};

pub fn sm3_hash(data: &[u8]) -> [u8; 32] {
    let mut h = Sm3::new();
    h.update(data);
    h.finalize().into()
}

fn hash_password_bytes(password: &str, salt: &[u8; 16]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(16 + password.len());
    buf.extend_from_slice(salt);
    buf.extend_from_slice(password.as_bytes());
    sm3_hash(&buf)
}

pub fn hash_password(password: &str, salt: &[u8; 16]) -> String {
    hash_password_bytes(password, salt)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub fn verify_password(password: &str, salt_hex: &str, hash_hex: &str) -> bool {
    let Some(salt) = hex::decode(salt_hex).ok() else {
        return false;
    };
    if salt.len() != 16 {
        return false;
    }
    let mut s = [0u8; 16];
    s.copy_from_slice(&salt);
    let Some(expected) = hex::decode(hash_hex).ok() else {
        return false;
    };
    ct_eq(&hash_password_bytes(password, &s), &expected)
}
