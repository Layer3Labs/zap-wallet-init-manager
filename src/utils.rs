// zap-wallet-init-manager/src/utils.rs

use fuels::prelude::*;
use fuels::accounts::wallet::Wallet;
use fuels::accounts::signers::private_key::PrivateKeySigner;
use fuels::crypto::SecretKey;
use std::str::FromStr;

use crate::UnlockedWallet;

/// Get a test wallet from environment or use default
pub fn get_test_wallet(provider: &Provider, env_key: &str) -> UnlockedWallet {
    let secret = std::env::var(env_key).unwrap_or_else(|_| {
        // Default test wallet secret
        "0x0000000000000000000000000000000000000000000000000000000000000001".to_string()
    });

    let signer = PrivateKeySigner::new(
        SecretKey::from_str(&secret).expect("Invalid secret key")
    );

    Wallet::new(signer, provider.clone())
}

/// Create multiple test wallets from environment
pub fn create_test_wallets(provider: &Provider, count: usize) -> Vec<UnlockedWallet> {
    let mut wallets = Vec::new();

    for i in 1..=count {
        let env_key = format!("EOA_PRIVATE_KEY_{}", i);
        if let Ok(secret_str) = std::env::var(&env_key) {
            match SecretKey::from_str(&secret_str) {
                Ok(secret_key) => {
                    let signer = PrivateKeySigner::new(secret_key);
                    let wallet = Wallet::new(signer, provider.clone());
                    wallets.push(wallet);
                }
                Err(e) => {
                    tracing::error!("Failed to parse {}: {}", env_key, e);
                }
            }
        }
    }

    // If no wallets loaded, create random ones
    if wallets.is_empty() {
        tracing::info!("No EOA wallets found in environment, creating {} random test wallets", count);
        for _ in 0..count {
            let mut rng = rand::thread_rng();
            let secret_key = SecretKey::random(&mut rng);
            let signer = PrivateKeySigner::new(secret_key);
            let wallet = Wallet::new(signer, provider.clone());
            wallets.push(wallet);
        }
    }

    wallets
}