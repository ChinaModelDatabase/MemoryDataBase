use anyhow::{anyhow, Result};
use rand::RngCore;
use sm4_gcm::{sm4_gcm_decrypt, sm4_gcm_encrypt, Sm4Key};

use crate::sm3_auth::sm3_hash;

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const SM4_KEY_LEN: usize = 16;

fn derive_sm4_key(file_key: &[u8; 32]) -> Sm4Key {
    // File keys are 32 bytes (storage-key-file), while SM4 uses 128-bit keys.
    // Derive via SM3 and keep the first 16 bytes.
    let h = sm3_hash(file_key);
    let mut sm4_key = [0u8; SM4_KEY_LEN];
    sm4_key.copy_from_slice(&h[..SM4_KEY_LEN]);
    Sm4Key(sm4_key)
}

pub fn sm4_gcm_seal(key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>> {
    let mut nonce = [0u8; NONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce);
    let sm4_key = derive_sm4_key(key);
    let ct = sm4_gcm_encrypt(&sm4_key, &nonce, plaintext);
    let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

pub fn sm4_gcm_open(key: &[u8; 32], sealed: &[u8]) -> Result<Vec<u8>> {
    if sealed.len() < NONCE_LEN + TAG_LEN {
        return Err(anyhow!("sealed data too short"));
    }
    let (nonce, rest) = sealed.split_at(NONCE_LEN);
    let sm4_key = derive_sm4_key(key);
    sm4_gcm_decrypt(&sm4_key, nonce, rest)
        .map_err(|e| anyhow!("sm4 decrypt (bad key or corrupted data): {e}"))
}
