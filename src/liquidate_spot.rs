//! One-shot spot liquidation binary.
//!
//! Given a Drift user account pubkey, fetches the user, gets a Jupiter or Titan quote,
//! builds a spot liquidation tx, and sends it.
//!
//! Usage:
//!   cargo run --bin liquidate_spot -- <USER_ACCOUNT_PUBKEY>
//!   cargo run --bin liquidate_spot -- --user-account <PUBKEY> [OPTIONS]
//!
//! Requires: BOT_PRIVATE_KEY, RPC_URL (optional, defaults to mainnet).

use std::sync::Arc;

mod filler;
mod http;
mod liquidator;
mod util;

use crate::filler::{TxSender, TxWorker};
use crate::http::Metrics;
use crate::liquidator::run_spot_liquidation;
use clap::Parser;
use drift_rs::{
    market_state::MarketStateData,
    types::{MarketId, SpotBalanceType},
    DriftClient, MarketState, RpcClient, Wallet,
};
use drift_rs::{types::accounts::User, Pubkey};

const TARGET: &str = "liquidate_spot";

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "One-shot spot liquidation for a Drift user account"
)]
struct Args {
    /// Drift user account (subaccount) pubkey to liquidate
    #[arg(long, short, env = "USER_ACCOUNT")]
    pub user_account: String,
    /// RPC URL
    #[arg(long, env = "RPC_URL")]
    pub rpc_url: Option<String>,
    /// Use mainnet (default true)
    #[arg(long, env = "MAINNET", default_value = "true")]
    pub mainnet: bool,
    /// Priority fee in microlamports
    #[arg(long, default_value = "512")]
    pub priority_fee: u64,
    /// Compute unit limit for the liquidation tx
    #[arg(long, default_value = "400000")]
    pub cu_limit: u32,
    /// Dry run: build and simulate but do not send
    #[arg(long, env = "DRY_RUN", default_value = "false")]
    pub dry_run: bool,
    /// Keeper subaccount id (default 0)
    #[arg(long, default_value = "0")]
    pub sub_account_id: u16,
}

fn build_market_state(drift: &DriftClient) -> Arc<std::sync::RwLock<MarketState>> {
    let mut market_state = MarketStateData::default();
    market_state.pyth_oracle_diff_threshold_bps = 5;

    for market in drift.program_data().perp_market_configs() {
        market_state.set_perp_market(*market);
        if let Some(oracle) = drift
            .backend()
            .oracle_map()
            .get_by_market(&MarketId::perp(market.market_index))
        {
            market_state.set_perp_oracle_price(market.market_index, oracle.data);
        }
    }
    for market in drift.program_data().spot_market_configs() {
        market_state.set_spot_market(*market);
        if let Some(oracle) = drift
            .backend()
            .oracle_map()
            .get_by_market(&MarketId::spot(market.market_index))
        {
            market_state.set_spot_oracle_price(market.market_index, oracle.data);
        }
    }

    Arc::new(std::sync::RwLock::new(MarketState::new(market_state)))
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let _ = dotenv::dotenv();

    let args = Args::parse();
    let liquidatee = match args.user_account.parse::<Pubkey>() {
        Ok(pk) => pk,
        Err(_) => {
            log::error!(target: TARGET, "invalid user account pubkey: {}", args.user_account);
            std::process::exit(1);
        }
    };

    let wallet: Wallet = drift_rs::utils::load_keypair_multi_format(
        &std::env::var("BOT_PRIVATE_KEY").expect("BOT_PRIVATE_KEY must be set"),
    )
    .expect("load BOT_PRIVATE_KEY")
    .into();

    let rpc_url = args.rpc_url.unwrap_or_else(|| {
        if args.mainnet {
            "https://api.mainnet-beta.solana.com".to_string()
        } else {
            "https://api.devnet.solana.com".to_string()
        }
    });

    let context = if args.mainnet {
        drift_rs::types::Context::MainNet
    } else {
        drift_rs::types::Context::DevNet
    };

    log::info!(
        target: TARGET,
        "connecting: rpc={}, liquidatee={:?}, dry_run={}",
        rpc_url,
        liquidatee,
        args.dry_run
    );

    let drift = DriftClient::new(context, RpcClient::new(rpc_url), wallet)
        .await
        .expect("DriftClient init");

    let keeper_subaccount = drift.wallet.sub_account(args.sub_account_id);

    // Fetch user account
    let user_account = match drift.try_get_account::<User>(&liquidatee) {
        Ok(u) => u,
        Err(e) => {
            log::error!(target: TARGET, "user account not found: {:?} - {}", liquidatee, e);
            std::process::exit(1);
        }
    };

    let borrow_count = user_account
        .spot_positions
        .iter()
        .filter(|p| matches!(p.balance_type, SpotBalanceType::Borrow) && !p.is_available())
        .count();
    if borrow_count == 0 {
        log::info!(target: TARGET, "user has no spot borrow positions to liquidate");
        return;
    }
    log::info!(target: TARGET, "user has {} spot borrow position(s)", borrow_count);

    let market_state = build_market_state(&drift);
    let metrics = Arc::new(Metrics::new());
    let tx_worker = TxWorker::new(drift.clone(), Arc::clone(&metrics), args.dry_run);
    let rt = tokio::runtime::Handle::current();
    let tx_sender = tx_worker.run(rt);

    let slot = drift.rpc().get_slot().await.unwrap_or(0);

    run_spot_liquidation(
        drift,
        metrics,
        market_state,
        keeper_subaccount,
        liquidatee,
        Arc::new(user_account),
        tx_sender,
        args.priority_fee,
        args.cu_limit,
        slot,
    )
    .await;

    if args.dry_run {
        log::info!(target: TARGET, "dry run: no tx sent");
    } else {
        log::info!(target: TARGET, "liquidation tx(s) submitted; waiting for worker to send");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}
