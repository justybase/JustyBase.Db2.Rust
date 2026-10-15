//! DRDA EUSRIDPWD (security mechanism 9) helpers: 256-bit Diffie-Hellman key
//! agreement and DES-CBC/PKCS#5 password and userid encryption.
//!
//! The prime, generator, exponent length, key/IV derivation, and padding match
//! the published DRDA values used by Apache Derby's EncryptionManager and
//! DecryptionManager. Modular exponentiation is delegated to `crypto-bigint`
//! (fixed-width Montgomery form), whose running time does not depend on the
//! bit pattern of the exponent, so the private key is not leaked through timing.

use crypto_bigint::{
    Odd, Uint,
    modular::{FixedMontyForm, FixedMontyParams},
    prelude::CtLt,
};
use des::Des;
use des::cipher::{Block, BlockEncrypt, KeyInit};
use ring::rand::{SecureRandom, SystemRandom};
use std::fmt;
use std::sync::LazyLock;
use zeroize::{Zeroize, Zeroizing};

/// Fixed-width 256-bit unsigned integer used for all Diffie-Hellman arithmetic.
type U256 = Uint<4>;

/// A Diffie-Hellman private exponent. Its limbs are overwritten when the value is dropped.
struct PrivateExponent(U256);

impl PrivateExponent {
    fn wipe(&mut self) {
        self.0.as_mut_words().zeroize();
    }
}

impl Drop for PrivateExponent {
    fn drop(&mut self) {
        self.wipe();
    }
}

const PRIME_LIMBS: [u64; 4] = [
    0x1e4c7a0d8c16b3e3,
    0xbff5b3a4ca0d60bc,
    0x947ab31f0f6846a1,
    0xc62112d73ee613f0,
];
const BASE_LIMBS: [u64; 4] = [
    0x21e256aed9ea34e4,
    0xcf071edcec5f626e,
    0x42c86c9114603fde,
    0x4690fa1f7b9e1d44,
];

/// Errors raised while applying DRDA SECMEC 9 cryptography.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Secmec9Error {
    /// The private exponent is outside `[2, p-2]`.
    InvalidPrivateKey,
    /// The peer public key is more than 32 bytes, or outside `(1, p-1)`.
    InvalidPublicKey,
    /// The server security token is too short to contain the 8-byte IV at offset 12.
    ShortServerToken,
    /// The DES cipher rejected the derived key.
    Cipher(String),
}

impl fmt::Display for Secmec9Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrivateKey => {
                write!(formatter, "DB2 EUSRIDPWD private key is out of range")
            }
            Self::InvalidPublicKey => {
                write!(formatter, "DB2 EUSRIDPWD peer public key is out of range")
            }
            Self::ShortServerToken => write!(
                formatter,
                "DB2 EUSRIDPWD server security token is too short"
            ),
            Self::Cipher(message) => write!(formatter, "DB2 EUSRIDPWD cipher error: {message}"),
        }
    }
}

impl std::error::Error for Secmec9Error {}

fn prime_value() -> U256 {
    U256::from_words(PRIME_LIMBS)
}

/// Montgomery parameters for the DH prime, computed once. The prime is odd, as Montgomery
/// reduction requires.
static MONTGOMERY_PARAMS: LazyLock<FixedMontyParams<4>> = LazyLock::new(|| {
    let modulus: Odd<U256> = Option::from(Odd::new(prime_value())).expect("DRDA prime is odd");
    FixedMontyParams::new(modulus)
});

/// Computes `base^exponent mod p` in constant time with respect to the exponent.
fn modpow(base: &U256, exponent: &U256) -> U256 {
    FixedMontyForm::new(base, &MONTGOMERY_PARAMS)
        .pow(exponent)
        .retrieve()
}

/// Accepts `[2, p-2]`, the valid range for DH private exponents and public keys.
fn in_dh_range(value: &U256) -> bool {
    let two = U256::from_u64(2);
    let upper = prime_value().wrapping_sub(&U256::ONE);
    bool::from(!value.ct_lt(&two)) && bool::from(value.ct_lt(&upper))
}

fn to_bytes(value: &U256) -> [u8; 32] {
    let mut output = [0u8; 32];
    output.copy_from_slice(&value.to_be_bytes());
    output
}

fn private_exponent(private_key: &[u8; 32]) -> Result<PrivateExponent, Secmec9Error> {
    let value = U256::from_be_slice(private_key);
    if !in_dh_range(&value) {
        return Err(Secmec9Error::InvalidPrivateKey);
    }
    Ok(PrivateExponent(value))
}

fn normalize_peer(public_key: &[u8]) -> Result<[u8; 32], Secmec9Error> {
    if public_key.len() > 32 {
        return Err(Secmec9Error::InvalidPublicKey);
    }
    let mut padded = [0u8; 32];
    padded[32 - public_key.len()..].copy_from_slice(public_key);
    Ok(padded)
}

fn validate_peer(padded: &[u8; 32]) -> Result<U256, Secmec9Error> {
    let peer = U256::from_be_slice(padded);
    if !in_dh_range(&peer) {
        return Err(Secmec9Error::InvalidPublicKey);
    }
    Ok(peer)
}

/// Generates a 255-bit Diffie-Hellman private exponent from the OS entropy pool. The key is
/// zeroized when the returned value is dropped.
pub fn generate_private_key() -> Result<Zeroizing<[u8; 32]>, Secmec9Error> {
    let random = SystemRandom::new();
    let mut bytes = Zeroizing::new([0u8; 32]);
    loop {
        random.fill(&mut *bytes).map_err(|_| {
            Secmec9Error::Cipher("operating system secure random generation failed".into())
        })?;
        bytes[0] &= 0x7f;
        if in_dh_range(&U256::from_be_slice(&*bytes)) {
            return Ok(bytes);
        }
    }
}

/// Computes `g^private mod p` as the 32-byte big-endian DRDA public key.
pub fn calculate_public_key(private_key: &[u8; 32]) -> Result<[u8; 32], Secmec9Error> {
    let exponent = private_exponent(private_key)?;
    Ok(to_bytes(&modpow(
        &U256::from_words(BASE_LIMBS),
        &exponent.0,
    )))
}

/// Derives the 32-byte shared secret from the peer public key and client private key. The
/// secret is zeroized when the returned value is dropped.
pub fn calculate_session_key(
    peer_public_key: &[u8],
    private_key: &[u8; 32],
) -> Result<Zeroizing<[u8; 32]>, Secmec9Error> {
    let exponent = private_exponent(private_key)?;
    let padded = normalize_peer(peer_public_key)?;
    let peer = validate_peer(&padded)?;
    Ok(Zeroizing::new(to_bytes(&modpow(&peer, &exponent.0))))
}

fn des_cbc_encrypt(key: &[u8], iv: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, Secmec9Error> {
    let cipher =
        Des::new_from_slice(key).map_err(|error| Secmec9Error::Cipher(error.to_string()))?;
    let mut padded = plaintext.to_vec();
    let padding = 8 - plaintext.len() % 8;
    padded.extend(std::iter::repeat_n(padding as u8, padding));
    let mut previous = [0u8; 8];
    previous.copy_from_slice(iv);
    let mut output = Vec::with_capacity(padded.len());
    for chunk in padded.as_chunks::<8>().0 {
        let mut block = Block::<Des>::clone_from_slice(chunk);
        for (index, byte) in block.iter_mut().enumerate() {
            *byte ^= previous[index];
        }
        cipher.encrypt_block(&mut block);
        output.extend_from_slice(&block);
        previous.copy_from_slice(&block);
    }
    Ok(output)
}

/// Encrypts one SECMEC 9 field with DES-CBC/PKCS#5 using the DRDA derived key and server-key IV.
pub fn encrypt(
    plaintext: &[u8],
    server_public_key: &[u8],
    private_key: &[u8; 32],
) -> Result<Vec<u8>, Secmec9Error> {
    if server_public_key.len() < 20 {
        return Err(Secmec9Error::ShortServerToken);
    }
    let padded = normalize_peer(server_public_key)?;
    let session_key = calculate_session_key(&padded, private_key)?;
    des_cbc_encrypt(&session_key[12..20], &padded[12..20], plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("hex"))
            .collect()
    }

    fn key_from_hex(hex: &str) -> [u8; 32] {
        from_hex(hex).try_into().expect("32-byte key")
    }

    #[test]
    fn secmec9_matches_independent_derby_and_openssl_vectors() {
        let client_private =
            key_from_hex("7f3a91c4d2e5b80716a4c9f0d3b8e2176a5c4d3e2f1908a7b6c5d4e3f2a1b0c9");
        let server_private =
            key_from_hex("1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a");
        let client_public =
            from_hex("7bed3dc4a1117944e7c7e85f820aab5fe0ce78ea0c03f95dfbd4189ed855b88a");
        let server_public =
            from_hex("2da57ee6ce7b212ededc6d07c1cdb28f6ed1ed5cbcade212e30d8ee51d69f3f1");
        let shared = from_hex("303a7d3ca869a26d920662ac3b57d8960aca6c8f6b69f01e8bfc66474e09af53");

        assert_eq!(
            calculate_public_key(&client_private).unwrap().to_vec(),
            client_public
        );
        assert_eq!(
            calculate_public_key(&server_private).unwrap().to_vec(),
            server_public
        );
        assert_eq!(
            calculate_session_key(&server_public, &client_private)
                .unwrap()
                .to_vec(),
            shared
        );

        let encrypted_password =
            encrypt(b"SecretPass123", &server_public, &client_private).unwrap();
        assert_eq!(hex(&encrypted_password), "a64a5e61af1a35a65d2bc8ec7ae550f4");
        let encrypted_userid = encrypt(b"DB2INST1", &server_public, &client_private).unwrap();
        assert_eq!(hex(&encrypted_userid), "e3241d6a67a9646ce5fafe9f9bae7acc");

        let session_key = calculate_session_key(&client_public, &server_private).unwrap();
        let expected_key = &session_key[12..20];
        let iv = &server_public[12..20];
        assert_eq!(
            des_cbc_decrypt(expected_key, iv, &encrypted_password),
            b"SecretPass123"
        );
        assert_eq!(
            des_cbc_decrypt(expected_key, iv, &encrypted_userid),
            b"DB2INST1"
        );
    }

    #[test]
    fn dh_range_accepts_only_two_through_p_minus_two() {
        let p_minus_one = prime_value().wrapping_sub(&U256::ONE);
        let p_minus_two = p_minus_one.wrapping_sub(&U256::ONE);

        assert!(private_exponent(&to_bytes(&U256::ZERO)).is_err());
        assert!(private_exponent(&to_bytes(&U256::ONE)).is_err());
        assert!(private_exponent(&to_bytes(&U256::from_u64(2))).is_ok());
        assert!(private_exponent(&to_bytes(&p_minus_two)).is_ok());
        assert!(private_exponent(&to_bytes(&p_minus_one)).is_err());
        assert!(validate_peer(&to_bytes(&U256::from_u64(2))).is_ok());
        assert!(validate_peer(&to_bytes(&p_minus_one)).is_err());
    }

    #[test]
    fn encrypt_rejects_a_server_token_shorter_than_the_iv_offset() {
        let client_private =
            key_from_hex("7f3a91c4d2e5b80716a4c9f0d3b8e2176a5c4d3e2f1908a7b6c5d4e3f2a1b0c9");
        let short_token = [0x11u8; 19];
        assert_eq!(
            encrypt(b"SecretPass123", &short_token, &client_private),
            Err(Secmec9Error::ShortServerToken)
        );
    }

    #[test]
    fn generated_private_keys_are_in_range() {
        for _ in 0..8 {
            let key = generate_private_key().unwrap();
            assert!(private_exponent(&key).is_ok());
            assert_eq!(key[0] & 0x80, 0, "top bit is cleared by generation");
        }
    }

    #[test]
    fn private_exponent_wipe_clears_every_limb() {
        let mut exponent = PrivateExponent(U256::from_u64(0x1234_5678));
        exponent.wipe();
        assert_eq!(exponent.0.as_words(), &[0; 4]);
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn des_cbc_decrypt(key: &[u8], iv: &[u8], ciphertext: &[u8]) -> Vec<u8> {
        use des::cipher::BlockDecrypt;
        let cipher = Des::new_from_slice(key).unwrap();
        let mut previous = [0u8; 8];
        previous.copy_from_slice(iv);
        let mut output = Vec::with_capacity(ciphertext.len());
        for chunk in ciphertext.as_chunks::<8>().0 {
            let mut block = Block::<Des>::clone_from_slice(chunk);
            let ciphertext_block = block;
            cipher.decrypt_block(&mut block);
            for (index, byte) in block.iter_mut().enumerate() {
                *byte ^= previous[index];
            }
            previous.copy_from_slice(&ciphertext_block);
            output.extend_from_slice(&block);
        }
        let padding = *output.last().expect("padded output") as usize;
        output.truncate(output.len() - padding);
        output
    }
}
