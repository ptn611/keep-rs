use std::{
    collections::HashSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use drift_rs::{
    constants::{
        perp_market_index_to_pyth_lazer_feed_id, pyth_lazer_feed_id_to_perp_market_index,
        pyth_lazer_feed_id_to_spot_market_index, spot_market_index_to_pyth_lazer_feed_id,
    },
    dlob::{L3Order, MakerCrosses},
    types::{MarketId, MarketType},
    Pubkey,
};
use futures_util::StreamExt;
use pyth_lazer_client::AnyResponse;
use pyth_lazer_protocol::{
    message::Message,
    payload::{PayloadData, PayloadPropertyValue},
    router::{
        Channel, DeliveryFormat, FixedRate, Format, JsonBinaryEncoding, PriceFeedId,
        PriceFeedProperty, SubscriptionParams, SubscriptionParamsRepr, TimestampUs,
    },
    subscription::{Response, SubscribeRequest, SubscriptionId},
};
use solana_sdk::signature::Signature;

pub struct OrderSlotLimiter<const N: usize> {
    slots: [Vec<u32>; N],
    generations: [u64; N],
}

impl<const N: usize> OrderSlotLimiter<N> {
    pub fn new() -> Self {
        let slots = std::array::from_fn(|_| Vec::new());
        let generations = [0; N];
        Self { slots, generations }
    }

    pub fn allow_event(&mut self, g: u64, id: u32) -> bool {
        let idx = (g % N as u64) as usize;

        // Replace old generation
        if self.generations[idx] != g {
            self.slots[idx].clear();
            self.generations[idx] = g;
        }

        // Count occurrences of id in generations g - 1 to g - 4
        let mut count = 0;
        for i in 2..=4 {
            let past_g = g.saturating_sub(i);
            let past_idx = (past_g % N as u64) as usize;

            if self.generations[past_idx] == past_g {
                if self.slots[past_idx].binary_search(&id).is_ok() {
                    count += 1;
                    if count >= 1 {
                        // Already appeared once, so this would be the second time
                        return false;
                    }
                }
            }
        }

        // Insert in sorted order
        let slot = &mut self.slots[idx];
        match slot.binary_search(&id) {
            Ok(_) => false, // Already present — shouldn't happen
            Err(pos) => {
                slot.insert(pos, id);
                true
            }
        }
    }

    pub fn _check_event(&self, g: u64, id: u32) -> bool {
        // Check generations g - 1 and g - 4
        for i in 1..=4 {
            let past_g = g.saturating_sub(i);
            let past_idx = (past_g % N as u64) as usize;

            if self.generations[past_idx] == past_g {
                if self.slots[past_idx].binary_search(&id).is_ok() {
                    return false;
                }
            }
        }

        true
    }
}

#[derive(Clone, Default, Debug)]
pub enum TxIntent {
    #[default]
    None,
    AuctionFill {
        _taker_order_id: u32,
        has_trigger: bool,
        maker_crosses: MakerCrosses,
    },
    SwiftFill {
        maker_crosses: MakerCrosses,
    },
    _VAMMTakerFill {
        slot: u64,
        _market_index: u16,
        _maker_order_id: u32,
    },
    /// limit orders crossed
    LimitUncross {
        slot: u64,
        _market_index: u16,
        _taker_order_id: u32,
        _maker_order_id: u32,
    },
    LiquidateWithFill {
        _market_index: u16,
        liquidatee: Pubkey,
        slot: u64,
    },
    LiquidatePerp {
        _market_index: u16,
        liquidatee: Pubkey,
        slot: u64,
    },
    _LiquidatePerpPnlForDeposit {
        perp_market_index: u16,
        spot_market_index: u16,
        liquidatee: Pubkey,
        slot: u64,
    },
    _LiquidateBorrowForPerpPnl {
        perp_market_index: u16,
        spot_market_index: u16,
        liquidatee: Pubkey,
        slot: u64,
    },
    LiquidateSpot {
        _asset_market_index: u16,
        _liability_market_index: u16,
        liquidatee: Pubkey,
        slot: u64,
    },
    Derisk {
        _market_index: u16,
        _subaccount: Pubkey,
    },
    SettlePnl {
        _market_index: u16,
        _subaccount: Pubkey,
    },
}

impl TxIntent {
    pub fn label(&self) -> &'static str {
        match self {
            TxIntent::None => "none",
            TxIntent::AuctionFill { maker_crosses, .. } => {
                if maker_crosses.has_vamm_cross {
                    "auction_fill_vamm"
                } else {
                    "auction_fill"
                }
            }
            TxIntent::SwiftFill { maker_crosses, .. } => {
                if maker_crosses.has_vamm_cross {
                    "swift_fill"
                } else {
                    "swift_fill_vamm"
                }
            }
            TxIntent::LimitUncross { .. } => "limit_uncross",
            TxIntent::_VAMMTakerFill { .. } => "vamm_taker",
            TxIntent::LiquidateWithFill { .. } => "liq_with_fill",
            TxIntent::LiquidatePerp { .. } => "liq_perp",
            TxIntent::_LiquidatePerpPnlForDeposit { .. } => "liq_perp_pnl_for_deposit",
            TxIntent::_LiquidateBorrowForPerpPnl { .. } => "liq_borrow_for_perp_pnl",
            TxIntent::LiquidateSpot { .. } => "liq_spot",
            TxIntent::Derisk { .. } => "derisk",
            TxIntent::SettlePnl { .. } => "settle_pnl",
        }
    }

    pub fn expected_fill_count(&self) -> usize {
        match self {
            TxIntent::None => 0,
            TxIntent::AuctionFill { maker_crosses, .. } => {
                maker_crosses.orders.len() + if maker_crosses.has_vamm_cross { 1 } else { 0 }
            }
            TxIntent::SwiftFill { maker_crosses, .. } => {
                maker_crosses.orders.len() + if maker_crosses.has_vamm_cross { 1 } else { 0 }
            }
            TxIntent::_VAMMTakerFill { .. } => 1,
            TxIntent::LimitUncross { .. } => 1,
            TxIntent::LiquidateWithFill { .. } => 1,
            TxIntent::LiquidatePerp { .. } => 0,
            TxIntent::_LiquidatePerpPnlForDeposit { .. } => 0,
            TxIntent::_LiquidateBorrowForPerpPnl { .. } => 0,
            TxIntent::LiquidateSpot { .. } => 0,
            TxIntent::Derisk { .. } => 0,
            TxIntent::SettlePnl { .. } => 0,
        }
    }

    /// true if tx was expected to trigger the taker order
    pub fn expected_trigger(&self) -> bool {
        match self {
            TxIntent::AuctionFill { has_trigger, .. } => *has_trigger,
            _ => false,
        }
    }

    pub fn crosses_and_slot(&self) -> (Vec<(L3Order, u64)>, u64) {
        match self {
            TxIntent::None => (vec![], 0),
            TxIntent::AuctionFill { maker_crosses, .. } => {
                (maker_crosses.orders.to_vec(), maker_crosses.slot)
            }
            TxIntent::SwiftFill { maker_crosses, .. } => {
                (maker_crosses.orders.to_vec(), maker_crosses.slot)
            }
            Self::_VAMMTakerFill { slot, .. } => (vec![], *slot),
            Self::LimitUncross { slot, .. } => (vec![], *slot),
            Self::LiquidateWithFill { slot, .. } => (vec![], *slot),
            Self::LiquidatePerp { slot, .. } => (vec![], *slot),
            Self::_LiquidatePerpPnlForDeposit { slot, .. } => (vec![], *slot),
            Self::_LiquidateBorrowForPerpPnl { slot, .. } => (vec![], *slot),
            Self::LiquidateSpot { slot, .. } => (vec![], *slot),
            TxIntent::Derisk { .. } => (vec![], 0),
            TxIntent::SettlePnl { .. } => (vec![], 0),
        }
    }

    pub fn slot(&self) -> Option<u64> {
        match self {
            Self::_VAMMTakerFill { slot, .. }
            | Self::LimitUncross { slot, .. }
            | Self::LiquidateWithFill { slot, .. }
            | Self::LiquidatePerp { slot, .. }
            | Self::_LiquidatePerpPnlForDeposit { slot, .. }
            | Self::_LiquidateBorrowForPerpPnl { slot, .. }
            | Self::LiquidateSpot { slot, .. } => Some(*slot),
            _ => None,
        }
    }

    /// Returns the liquidatee pubkey if this is a liquidation intent
    pub fn liquidatee(&self) -> Option<Pubkey> {
        match self {
            Self::LiquidateWithFill { liquidatee, .. }
            | Self::LiquidatePerp { liquidatee, .. }
            | Self::_LiquidatePerpPnlForDeposit { liquidatee, .. }
            | Self::_LiquidateBorrowForPerpPnl { liquidatee, .. }
            | Self::LiquidateSpot { liquidatee, .. } => Some(*liquidatee),
            _ => None,
        }
    }

    /// Returns true if this intent is a liquidation type
    pub fn is_liquidation(&self) -> bool {
        matches!(
            self,
            Self::LiquidateWithFill { .. }
                | Self::LiquidatePerp { .. }
                | Self::_LiquidatePerpPnlForDeposit { .. }
                | Self::_LiquidateBorrowForPerpPnl { .. }
                | Self::LiquidateSpot { .. }
        )
    }
}

#[derive(Clone, Default, Debug)]
pub struct PendingTxMeta {
    pub signature: Signature,
    pub intent: TxIntent,
    pub cu_limit: u64,
    pub _ts: u64,
}

impl PendingTxMeta {
    pub fn new(sig: Signature, intent: TxIntent, cu_limit: u64) -> Self {
        Self {
            signature: sig,
            _ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            intent,
            cu_limit,
        }
    }
}

/// Circular buffer for pending transactions or similar FIFO workloads.
///
/// Usage example:
/// ```
/// let mut buf: PendingTxs<1024> = PendingTxs::new();
/// buf.insert(meta);
/// let confirmed = buf.confirm(|m| m.signature == sig);
/// ```
pub struct PendingTxs<const N: usize> {
    buffer: Box<[PendingTxMeta; N]>,
    head: usize,
    tail: usize,
    size: usize,
}

impl<const N: usize> PendingTxs<N> {
    pub fn new() -> Self {
        Self {
            buffer: Box::new([(); N].map(|_| PendingTxMeta::default())),
            head: 0,
            tail: 0,
            size: 0,
        }
    }

    /// Insert a new item, overwriting the oldest if full.
    pub fn insert(&mut self, item: PendingTxMeta) {
        self.buffer[self.tail] = item;
        self.tail = (self.tail + 1) % N;
        if self.size == N {
            self.head = (self.head + 1) % N;
        } else {
            self.size += 1;
        }
    }

    /// Confirm and return the first item with matching signature.
    ///
    /// Returns Some(item) if found, else None.
    pub fn confirm(&mut self, sig: &Signature) -> Option<PendingTxMeta> {
        for i in 0..self.size {
            let idx = (self.head + i) % N;
            // TODO: check if overwritten entry is confirmed or not
            if self.buffer[idx].signature == *sig {
                return Some(self.buffer[idx].clone());
            }
        }
        None
    }
}

#[derive(Clone, Debug)]
pub struct PythPriceUpdate {
    pub market_type: MarketType,
    pub market_id: u16,
    pub feed_id: u32,
    pub price: u64,
    // original pyth message
    pub message: Vec<u8>,
    pub ts: TimestampUs,
}

fn fixed_rate(feed_id: u32) -> FixedRate {
    match feed_id {
        1 | 2 | 6 => FixedRate::MIN,
        10 => FixedRate::from_ms(50).unwrap(),
        _ => FixedRate::from_ms(200).unwrap(),
    }
}

// scale pyth lazer price into drift price precision
#[inline(always)]
fn to_price_precision(price: u64, feed_id: u32, market_type: MarketType) -> u64 {
    match feed_id {
        // https://docs.pyth.network/lazer/price-feed-ids
        // LAZER_1M
        9 => match market_type {
            MarketType::Perp => price * 100,    // -10 => -6 * 1M
            MarketType::Spot => price / 10_000, // -10 => -6
        },
        4 => price * 100, // -10 => -6 * 1M
        // LAZER_1K
        1578 | 2396 | 137 => match market_type {
            MarketType::Perp => price * 10,  // -10 => -6 * 1K
            MarketType::Spot => price / 100, // -8 => -6
        },
        _ => price / 100, // -8 => -6
    }
}

pub fn subscribe_price_feeds(
    mut cli: pyth_lazer_client::LazerClient,
    perp_market_ids: &[MarketId],
    spot_market_ids: &[MarketId],
    extra_feed_ids: &[u32],
) -> tokio::sync::mpsc::Receiver<PythPriceUpdate> {
    let mut feed_id_set = HashSet::new();

    for m in perp_market_ids {
        if let Some(fid) = perp_market_index_to_pyth_lazer_feed_id(m.index()) {
            feed_id_set.insert(fid);
        }
    }

    for m in spot_market_ids {
        if let Some(fid) = spot_market_index_to_pyth_lazer_feed_id(m.index()) {
            feed_id_set.insert(fid);
        }
    }

    let extra_feeds: HashSet<u32> = extra_feed_ids.iter().copied().collect();
    feed_id_set.extend(extra_feeds.iter().copied());

    let feed_ids: Vec<PriceFeedId> = feed_id_set.into_iter().map(PriceFeedId).collect();

    const MAX_RETRIES: u32 = 10;

    let (price_tx, price_rx) = tokio::sync::mpsc::channel(512);

    let mut retries = 0u32;
    tokio::spawn(async move {
        loop {
            let pyth_lazer_stream = match cli.start().await {
                Ok(stream) => stream,
                Err(err) => {
                    retries += 1;

                    if retries >= MAX_RETRIES {
                        log::error!(
                            target: "pyth",
                            "FATAL: feed connection failed after {MAX_RETRIES} attempts; closing price feed channel"
                        );
                        return;
                    } else {
                        let backoff = 2u64.pow(retries).min(30); // 2^retries seconds, capped at 30s
                        log::warn!(
                            target: "pyth",
                            "feed connection failed: {err:?}, retry {retries}/{MAX_RETRIES} in {backoff}s"
                        );
                        tokio::time::sleep(Duration::from_secs(backoff)).await;
                        continue;
                    }
                }
            };

            // sub per feed
            let mut sub_id = 0;
            for feed_id in feed_ids.iter() {
                let subscribe_request = SubscribeRequest {
                    subscription_id: SubscriptionId(sub_id),
                    params: SubscriptionParams::new(SubscriptionParamsRepr {
                        price_feed_ids: vec![*feed_id],
                        // drift program requires exponent + feed_update_timestamp to apply the update
                        properties: vec![
                            PriceFeedProperty::Price,
                            PriceFeedProperty::Exponent,
                            PriceFeedProperty::FeedUpdateTimestamp,
                        ],
                        delivery_format: DeliveryFormat::Binary,
                        json_binary_encoding: JsonBinaryEncoding::Hex,
                        parsed: false,
                        channel: Channel::FixedRate(fixed_rate(feed_id.0)),
                        formats: vec![Format::Solana],
                        ignore_invalid_feed_ids: false,
                    })
                    .expect("invalid subscription params"),
                };
                sub_id += 1;
                if let Err(err) = cli
                    .subscribe(pyth_lazer_protocol::subscription::Request::Subscribe(
                        subscribe_request,
                    ))
                    .await
                {
                    log::error!(target: "pyth", "pyth feed subscribe failed: {err:?}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }

            retries = 0u32; // retry on successful connect

            let mut stream = pyth_lazer_stream.boxed();
            while let Some(update) = stream.next().await {
                match update {
                    Ok(AnyResponse::Binary(outer)) => {
                        for message in outer.messages {
                            match message {
                                Message::Solana(solana) => {
                                    let mut buf = Vec::with_capacity(solana.payload.len() + 128);
                                    solana.serialize(&mut buf).expect("serialized");
                                    let data =
                                        PayloadData::deserialize_slice_le(&solana.payload).unwrap();

                    log::trace!(target: "pyth", "got update: {data:?}");
                    for f in data.feeds {
                        for p in f.properties {
                            if let PayloadPropertyValue::Price(Some(new_price)) = p
                            {
                                // TODO: bulk msg to avoid bouncing around tokio, bucket in some way, one message updates multiple markets...
                                let feed_id = f.feed_id.0;
                                let price: u64 = new_price.0.unsigned_abs().into();

                                if let Some(market_id) =
                                    pyth_lazer_feed_id_to_perp_market_index(feed_id)
                                {
                                    let scaled_price = to_price_precision(
                                        price,
                                        feed_id,
                                        MarketType::Perp,
                                    );
                                    let _ = price_tx.try_send(PythPriceUpdate {
                                        market_type: MarketType::Perp,
                                        market_id,
                                        feed_id,
                                        price: scaled_price,
                                        message: buf.clone(),
                                        ts: data.timestamp_us,
                                    });
                                }

                                if let Some(market_id) =
                                    pyth_lazer_feed_id_to_spot_market_index(feed_id)
                                {
                                    let scaled_price = to_price_precision(
                                        price,
                                        feed_id,
                                        MarketType::Spot,
                                    );
                                    let _ = price_tx.try_send(PythPriceUpdate {
                                        market_type: MarketType::Spot,
                                        market_id,
                                        feed_id,
                                        price: scaled_price,
                                        message: buf.clone(),
                                        ts: data.timestamp_us,
                                    });
                                }

                                // Extra feeds (cluster-specific): emit a synthetic
                                // update so the relayer ships them. drift-rs
                                // derives the oracle PDA from feed_id alone, so
                                // market_id is informational only.
                                if extra_feeds.contains(&feed_id)
                                    && pyth_lazer_feed_id_to_perp_market_index(
                                        feed_id,
                                    )
                                    .is_none()
                                    && pyth_lazer_feed_id_to_spot_market_index(
                                        feed_id,
                                    )
                                    .is_none()
                                {
                                    let scaled_price = to_price_precision(
                                        price,
                                        feed_id,
                                        MarketType::Spot,
                                    );
                                    let _ = price_tx.try_send(PythPriceUpdate {
                                        market_type: MarketType::Spot,
                                        market_id: u16::MAX,
                                        feed_id,
                                        price: scaled_price,
                                        message: buf.clone(),
                                        ts: data.timestamp_us,
                                    });
                                }
                            }
                        }
                    }
                }
                _ => (),
            }
        }
    }
    other => match other {
        Ok(AnyResponse::Json(Response::Subscribed(sub))) => {
            log::info!(
                target: "pyth",
                "subscribed feed {}",
                sub.subscription_id.0
            );
        }
        Ok(AnyResponse::Json(msg)) => {
            log::info!(target: "pyth", "control msg: {msg:?}");
        }
        Err(err) => {
            log::warn!(
                target: "pyth",
                "websocket error from pyth stream: {err:?}"
            );
        }
        Ok(other_ok) => {
            log::info!(target: "pyth", "non-binary msg: {other_ok:?}");
        }
    },
}
            }
            // stream ended, will retry
            retries += 1;
            if retries >= MAX_RETRIES {
                log::error!(
                    target: "pyth",
                    "FATAL: feed disconnected after {MAX_RETRIES} attempts; closing price feed channel"
                );
                return;
            }
            let backoff = 2u64.pow(retries).min(30);
            log::warn!(
                target: "pyth",
                "feed disconnected, retry {retries}/{MAX_RETRIES} in {backoff}s"
            );
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        }
    });

    price_rx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_price_precision_scales_per_feed_and_market() {
        // LAZER_1M feed: perp 10^-6 -> 10^-6 * 1M, spot 10^-10 -> 10^-6
        assert_eq!(to_price_precision(100, 9, MarketType::Perp), 10_000);
        assert_eq!(to_price_precision(1_000_000, 9, MarketType::Perp), 100_000_000);
        assert_eq!(to_price_precision(1_000_000, 9, MarketType::Spot), 100);
        assert_eq!(to_price_precision(10_000, 9, MarketType::Spot), 1);

        // feed 4: always *100 (10^-8 -> 10^-6 * 1M)
        assert_eq!(to_price_precision(100, 4, MarketType::Perp), 10_000);
        assert_eq!(to_price_precision(100, 4, MarketType::Spot), 10_000);

        // LAZER_1K feeds: perp 10^-5 -> 10^-6 * 1K, spot 10^-8 -> 10^-6
        for feed_id in [1578u32, 2396, 137] {
            assert_eq!(to_price_precision(100, feed_id, MarketType::Perp), 1_000);
            assert_eq!(to_price_precision(1_000_000, feed_id, MarketType::Spot), 10_000);
        }

        // default feed: 10^-8 -> 10^-6
        assert_eq!(to_price_precision(100, 999, MarketType::Perp), 1);
        assert_eq!(to_price_precision(1_000_000, 999, MarketType::Spot), 10_000);
        assert_eq!(to_price_precision(1_000_000, 999, MarketType::Perp), 10_000);

        // zero price
        assert_eq!(to_price_precision(0, 9, MarketType::Perp), 0);
        assert_eq!(to_price_precision(0, 999, MarketType::Spot), 0);
    }

    #[test]
    fn fixed_rate_maps_feeds_to_intervals() {
        assert_eq!(fixed_rate(1).value_ms(), FixedRate::MIN.value_ms());
        assert_eq!(fixed_rate(2).value_ms(), FixedRate::MIN.value_ms());
        assert_eq!(fixed_rate(6).value_ms(), FixedRate::MIN.value_ms());
        assert_eq!(fixed_rate(10).value_ms(), 50);
        assert_eq!(fixed_rate(11).value_ms(), 200);
        assert_eq!(fixed_rate(999).value_ms(), 200);
    }

    #[test]
    fn order_slot_limiter_allows_first_event_in_generation() {
        let mut limiter = OrderSlotLimiter::<40>::new();
        assert!(limiter.allow_event(10, 7));
    }

    #[test]
    fn order_slot_limiter_rejects_duplicate_in_same_generation() {
        let mut limiter = OrderSlotLimiter::<40>::new();
        assert!(limiter.allow_event(10, 7));
        assert!(!limiter.allow_event(10, 7));
    }

    #[test]
    fn order_slot_limiter_rejects_ids_from_g2_to_g4() {
        let mut limiter = OrderSlotLimiter::<40>::new();
        assert!(limiter.allow_event(10, 7));
        // g-2: rejected
        assert!(!limiter.allow_event(12, 7));
        // g-3: rejected
        assert!(!limiter.allow_event(13, 7));
        // g-4: rejected
        assert!(!limiter.allow_event(14, 7));
    }

    #[test]
    fn order_slot_limiter_allows_id_from_previous_generation_gap() {
        let mut limiter = OrderSlotLimiter::<40>::new();
        assert!(limiter.allow_event(10, 7));
        // g-1 is NOT checked by allow_event (only g-2..=g-4)
        assert!(limiter.allow_event(11, 7));
    }

    #[test]
    fn order_slot_limiter_reallows_after_past_window() {
        let mut limiter = OrderSlotLimiter::<40>::new();
        assert!(limiter.allow_event(10, 7));
        // g-5 = 10, outside the g-2..=g-4 window
        assert!(limiter.allow_event(15, 7));
    }

    #[test]
    fn order_slot_limiter_check_event_includes_g1() {
        let mut limiter = OrderSlotLimiter::<40>::new();
        assert!(limiter.allow_event(10, 7));
        assert!(!limiter._check_event(11, 7));
        assert!(limiter._check_event(15, 7));
    }

    #[test]
    fn order_slot_limiter_wraps_after_n_generations() {
        let mut limiter = OrderSlotLimiter::<40>::new();
        assert!(limiter.allow_event(0, 7));
        // 40 generations later the same bucket is recycled, generation is
        // replaced, and the old id is forgotten.
        assert!(limiter.allow_event(40, 7));
        // A distinct generation in the same bucket then stores its own ids.
        assert!(limiter.allow_event(80, 8));
        assert!(!limiter.allow_event(80, 8));
    }

    #[test]
    fn order_slot_limiter_distinct_ids_in_same_generation() {
        let mut limiter = OrderSlotLimiter::<40>::new();
        assert!(limiter.allow_event(10, 1));
        assert!(limiter.allow_event(10, 2));
        assert!(limiter.allow_event(10, 3));
    }

    #[test]
    fn pending_txs_fifo_confirm() {
        let mut pending = PendingTxs::<4>::new();
        let sigs: Vec<Signature> = (0..3).map(|_| Signature::new_unique()).collect();
        for (i, sig) in sigs.iter().enumerate() {
            pending.insert(PendingTxMeta::new(*sig, TxIntent::None, i as u64 + 1));
        }
        for (i, sig) in sigs.iter().enumerate() {
            let meta = pending.confirm(sig).expect("present");
            assert_eq!(meta.cu_limit, i as u64 + 1);
        }
    }

    #[test]
    fn pending_txs_confirm_missing_returns_none() {
        let mut pending = PendingTxs::<4>::new();
        assert!(pending.confirm(&Signature::new_unique()).is_none());
    }

    #[test]
    fn pending_txs_overwrites_oldest_when_full() {
        let mut pending = PendingTxs::<4>::new();
        let sigs: Vec<Signature> = (0..5).map(|_| Signature::new_unique()).collect();
        for sig in &sigs {
            pending.insert(PendingTxMeta::new(*sig, TxIntent::None, 0));
        }
        // size stays capped at N
        assert!(pending.confirm(&sigs[0]).is_none()); // overwritten
        for sig in &sigs[1..] {
            assert!(pending.confirm(sig).is_some());
        }
    }

    #[test]
    fn pending_txs_confirm_does_not_remove() {
        let mut pending = PendingTxs::<4>::new();
        let sig = Signature::new_unique();
        pending.insert(PendingTxMeta::new(sig, TxIntent::None, 0));
        assert!(pending.confirm(&sig).is_some());
        assert!(pending.confirm(&sig).is_some());
    }

    fn crosses(has_vamm: bool) -> MakerCrosses {
        MakerCrosses {
            has_vamm_cross: has_vamm,
            ..Default::default()
        }
    }

    #[test]
    fn tx_intent_label_mapping() {
        assert_eq!(TxIntent::None.label(), "none");
        assert_eq!(TxIntent::LimitUncross { slot: 1, _market_index: 0, _taker_order_id: 0, _maker_order_id: 0 }.label(), "limit_uncross");
        assert_eq!(TxIntent::AuctionFill { _taker_order_id: 0, has_trigger: false, maker_crosses: crosses(false) }.label(), "auction_fill");
        assert_eq!(TxIntent::AuctionFill { _taker_order_id: 0, has_trigger: false, maker_crosses: crosses(true) }.label(), "auction_fill_vamm");
        assert_eq!(TxIntent::LiquidateWithFill { _market_index: 0, liquidatee: Pubkey::default(), slot: 0 }.label(), "liq_with_fill");
        assert_eq!(TxIntent::LiquidatePerp { _market_index: 0, liquidatee: Pubkey::default(), slot: 0 }.label(), "liq_perp");
        assert_eq!(TxIntent::LiquidateSpot { _asset_market_index: 0, _liability_market_index: 0, liquidatee: Pubkey::default(), slot: 0 }.label(), "liq_spot");
        assert_eq!(TxIntent::Derisk { _market_index: 0, _subaccount: Pubkey::default() }.label(), "derisk");
        assert_eq!(TxIntent::SettlePnl { _market_index: 0, _subaccount: Pubkey::default() }.label(), "settle_pnl");

        // NOTE: suspected bug — SwiftFill labels appear inverted vs AuctionFill:
        // with a VAMM cross the label is "swift_fill" and without it is
        // "swift_fill_vamm". Tests assert current (as-written) behaviour.
        assert_eq!(TxIntent::SwiftFill { maker_crosses: crosses(false) }.label(), "swift_fill_vamm");
        assert_eq!(TxIntent::SwiftFill { maker_crosses: crosses(true) }.label(), "swift_fill");
    }

    #[test]
    fn tx_intent_expected_fill_count() {
        assert_eq!(TxIntent::None.expected_fill_count(), 0);
        assert_eq!(TxIntent::AuctionFill { _taker_order_id: 0, has_trigger: false, maker_crosses: crosses(false) }.expected_fill_count(), 0);
        assert_eq!(TxIntent::AuctionFill { _taker_order_id: 0, has_trigger: false, maker_crosses: crosses(true) }.expected_fill_count(), 1);
        assert_eq!(TxIntent::SwiftFill { maker_crosses: crosses(true) }.expected_fill_count(), 1);
        assert_eq!(TxIntent::LimitUncross { slot: 1, _market_index: 0, _taker_order_id: 0, _maker_order_id: 0 }.expected_fill_count(), 1);
        assert_eq!(TxIntent::LiquidateWithFill { _market_index: 0, liquidatee: Pubkey::default(), slot: 0 }.expected_fill_count(), 1);
        assert_eq!(TxIntent::LiquidatePerp { _market_index: 0, liquidatee: Pubkey::default(), slot: 0 }.expected_fill_count(), 0);
        assert_eq!(TxIntent::LiquidateSpot { _asset_market_index: 0, _liability_market_index: 0, liquidatee: Pubkey::default(), slot: 0 }.expected_fill_count(), 0);
        assert_eq!(TxIntent::Derisk { _market_index: 0, _subaccount: Pubkey::default() }.expected_fill_count(), 0);
    }

    #[test]
    fn tx_intent_expected_trigger() {
        let t = TxIntent::AuctionFill { _taker_order_id: 0, has_trigger: true, maker_crosses: crosses(false) };
        assert!(t.expected_trigger());
        let t = TxIntent::AuctionFill { _taker_order_id: 0, has_trigger: false, maker_crosses: crosses(false) };
        assert!(!t.expected_trigger());
        assert!(!TxIntent::LimitUncross { slot: 1, _market_index: 0, _taker_order_id: 0, _maker_order_id: 0 }.expected_trigger());
    }

    #[test]
    fn tx_intent_slot_liquidatee_and_is_liquidation() {
        let pk = Pubkey::new_unique();
        assert_eq!(TxIntent::None.slot(), None);
        assert_eq!(TxIntent::LimitUncross { slot: 42, _market_index: 0, _taker_order_id: 0, _maker_order_id: 0 }.slot(), Some(42));
        assert_eq!(TxIntent::LiquidateSpot { _asset_market_index: 0, _liability_market_index: 1, liquidatee: pk, slot: 7 }.slot(), Some(7));
        assert_eq!(TxIntent::LiquidateSpot { _asset_market_index: 0, _liability_market_index: 1, liquidatee: pk, slot: 7 }.liquidatee(), Some(pk));
        assert!(TxIntent::LiquidateWithFill { _market_index: 0, liquidatee: pk, slot: 0 }.is_liquidation());
        assert!(TxIntent::LiquidateSpot { _asset_market_index: 0, _liability_market_index: 1, liquidatee: pk, slot: 0 }.is_liquidation());
        assert!(!TxIntent::AuctionFill { _taker_order_id: 0, has_trigger: false, maker_crosses: crosses(false) }.is_liquidation());
        assert!(!TxIntent::None.is_liquidation());
    }
}
