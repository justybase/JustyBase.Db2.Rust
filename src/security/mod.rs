//! DRDA EUSRIDPWD (security mechanism 9) helpers: 256-bit Diffie-Hellman key
//! agreement and DES-CBC/PKCS#5 password and userid encryption.
//!
//! The prime, generator, exponent length, key/IV derivation, and padding match
//! the published DRDA values used by Apache Derby's EncryptionManager and
//! DecryptionManager. A tiny fixed-limb modular arithmetic implementation
//! keeps the runtime dependency surface unchanged apart from the DES cipher.

use des::Des;
use des::cipher::{Block, BlockEncrypt, KeyInit};
use ring::rand::{SecureRandom, SystemRandom};
use std::fmt;

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

#[derive(Clone, Copy, PartialEq, Eq, Default)]
struct U256([u64; 4]);

impl U256 {
    const ONE: Self = Self([1, 0, 0, 0]);

    fn from_be_bytes(bytes: &[u8; 32]) -> Self {
        let mut limbs = [0u64; 4];
        for (index, limb) in limbs.iter_mut().enumerate() {
            let start = 32 - 8 * (index + 1);
            *limb = u64::from_be_bytes(bytes[start..start + 8].try_into().expect("8-byte limb"));
        }
        Self(limbs)
    }

    fn to_be_bytes(self) -> [u8; 32] {
        let mut output = [0u8; 32];
        for index in 0..4 {
            output[32 - 8 * (index + 1)..32 - 8 * index]
                .copy_from_slice(&self.0[index].to_be_bytes());
        }
        output
    }

    fn bit(self, index: usize) -> bool {
        (self.0[index / 64] >> (index % 64)) & 1 == 1
    }

    fn lt(self, other: Self) -> bool {
        for index in (0..4).rev() {
            if self.0[index] != other.0[index] {
                return self.0[index] < other.0[index];
            }
        }
        false
    }

    fn add_carry(self, other: Self) -> (Self, bool) {
        let mut output = [0u64; 4];
        let mut carry = false;
        for (index, slot) in output.iter_mut().enumerate() {
            let (sum, carry_a) = self.0[index].overflowing_add(other.0[index]);
            let (sum, carry_b) = sum.overflowing_add(u64::from(carry));
            *slot = sum;
            carry = carry_a || carry_b;
        }
        (Self(output), carry)
    }

    fn sub_wrapping(self, other: Self) -> Self {
        let mut output = [0u64; 4];
        let mut borrow = false;
        for (index, slot) in output.iter_mut().enumerate() {
            let (difference, borrow_a) = self.0[index].overflowing_sub(other.0[index]);
            let (difference, borrow_b) = difference.overflowing_sub(u64::from(borrow));
            *slot = difference;
            borrow = borrow_a || borrow_b;
        }
        Self(output)
    }

    fn add_mod(self, other: Self, modulus: Self) -> Self {
        let (sum, carry) = self.add_carry(other);
        if carry || !sum.lt(modulus) {
            sum.sub_wrapping(modulus)
        } else {
            sum
        }
    }

    /// Schoolbook double-and-add modular multiplication. The operands are
    /// below the 256-bit modulus, so 256 shift/add steps are sufficient.
    fn mul_mod(self, other: Self, modulus: Self) -> Self {
        let mut result = Self::default();
        let mut addend = self;
        for index in 0..256 {
            if other.bit(index) {
                result = result.add_mod(addend, modulus);
            }
            addend = addend.add_mod(addend, modulus);
        }
        result
    }

    fn pow_mod(self, exponent: Self, modulus: Self) -> Self {
        let mut result = Self::ONE;
        let mut base = self;
        for index in 0..256 {
            if exponent.bit(index) {
                result = result.mul_mod(base, modulus);
            }
            base = base.mul_mod(base, modulus);
        }
        result
    }
}

fn prime() -> U256 {
    U256(PRIME_LIMBS)
}

fn base() -> U256 {
    U256(BASE_LIMBS)
}

fn private_exponent(private_key: &[u8; 32]) -> Result<U256, Secmec9Error> {
    let value = U256::from_be_bytes(private_key);
    let p = prime();
    if value.lt(U256([2, 0, 0, 0])) || !value.lt(p.sub_wrapping(U256::ONE)) {
        return Err(Secmec9Error::InvalidPrivateKey);
    }
    Ok(value)
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
    let peer = U256::from_be_bytes(padded);
    let p = prime();
    if peer.lt(U256([2, 0, 0, 0])) || !peer.lt(p.sub_wrapping(U256::ONE)) {
        return Err(Secmec9Error::InvalidPublicKey);
    }
    Ok(peer)
}

/// Generates a 255-bit Diffie-Hellman private exponent from the OS entropy pool.
pub fn generate_private_key() -> Result<[u8; 32], Secmec9Error> {
    let random = SystemRandom::new();
    let p = prime();
    loop {
        let mut bytes = [0u8; 32];
        random.fill(&mut bytes).map_err(|_| {
            Secmec9Error::Cipher("operating system secure random generation failed".into())
        })?;
        bytes[0] &= 0x7f;
        let candidate = U256::from_be_bytes(&bytes);
        if !candidate.lt(U256([2, 0, 0, 0])) && candidate.lt(p.sub_wrapping(U256::ONE)) {
            return Ok(bytes);
        }
    }
}

/// Computes `g^private mod p` as the 32-byte big-endian DRDA public key.
pub fn calculate_public_key(private_key: &[u8; 32]) -> Result<[u8; 32], Secmec9Error> {
    let exponent = private_exponent(private_key)?;
    Ok(base().pow_mod(exponent, prime()).to_be_bytes())
}

/// Derives the 32-byte shared secret from the peer public key and client private key.
pub fn calculate_session_key(
    peer_public_key: &[u8],
    private_key: &[u8; 32],
) -> Result<[u8; 32], Secmec9Error> {
    let exponent = private_exponent(private_key)?;
    let padded = normalize_peer(peer_public_key)?;
    let peer = validate_peer(&padded)?;
    Ok(peer.pow_mod(exponent, prime()).to_be_bytes())
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
    let padded = normalize_peer(server_public_key)?;
    if padded.len() < 20 {
        return Err(Secmec9Error::ShortServerToken);
    }
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
