"""Offline proposal: chain/transfer veto for Korean caution shorts. No orders or book writes."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Any


@dataclass(frozen=True)
class Config:
    version: str = 'korean-onchain-guard-proposal-v1'
    chain_id: str = 'interwoven-1'
    native_denom: str = 'uinit'
    observation_max_age_seconds: int = 600
    block_max_age_seconds: int = 600
    require_native_exchange_flow: bool = False


def utc(value: str) -> datetime:
    value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if value.tzinfo is None:
        raise ValueError('Explicit UTC offset required')
    return value.astimezone(timezone.utc)


def _available(obs: Any, cutoff: datetime, config: Config, *, event_fresh=False) -> bool:
    try:
        if not isinstance(obs, dict) or obs.get('provenance') != 'live' or not obs.get('source'):
            return False
        if obs.get('asset_chain_id') != config.chain_id or obs.get('asset_denom') != config.native_denom:
            return False
        available, event = utc(obs['available_at_utc']), utc(obs['event_at_utc'])
        if event > available or available > cutoff or event > cutoff:
            return False
        if (cutoff-available).total_seconds() > config.observation_max_age_seconds:
            return False
        return not event_fresh or (cutoff-event).total_seconds() <= config.observation_max_age_seconds
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def assess(features: dict, cutoff_utc: str, config: Config = Config()) -> dict:
    """A pass delegates to the baseline; it never creates an entry or claims profitability.

    Historical block/transaction timestamps cannot replace actual receipt times.
    Deposit-only suspension is normal for caution notices and is not a blanket veto.
    """
    cutoff = utc(cutoff_utc)

    def decision(action, reason):
        return {'version': config.version, 'decision_at_utc': cutoff.isoformat(),
                'action': action, 'allows_baseline_review': action == 'pass_to_baseline',
                'reason': reason, 'paper_only': True, 'creates_trade': False}

    transfer = features.get('transfer_status')
    transfer_ok = _available(transfer, cutoff, config, event_fresh=True)
    if transfer_ok and transfer.get('value') == 'both_suspended':
        return decision('veto_short', 'deposits_and_withdrawals_suspended')

    chain = features.get('chain_tip')
    if not _available(chain, cutoff, config):
        return decision('defer', 'chain_observation_unavailable_at_decision')
    value = chain.get('value')
    if not isinstance(value, dict) or value.get('chain_id') != config.chain_id or value.get('denom') != config.native_denom:
        return decision('defer', 'native_asset_identity_unverified')
    if not isinstance(value.get('height'), int) or isinstance(value['height'], bool) or value['height'] <= 0:
        return decision('defer', 'invalid_block_height')
    lag = (cutoff-utc(chain['event_at_utc'])).total_seconds()
    if lag > config.block_max_age_seconds:
        # One stale operator does not prove a global halt; it is enough to defer entry.
        return decision('defer', 'chain_tip_stale_do_not_assume_zero_flow')
    if not transfer_ok or transfer.get('value') not in ('open', 'deposit_only_suspended'):
        return decision('defer', 'current_transfer_status_unknown')

    if config.require_native_exchange_flow:
        flow = features.get('exchange_flow')
        if not _available(flow, cutoff, config, event_fresh=True):
            return decision('defer', 'native_flow_unavailable_at_decision')
        value = flow.get('value')
        if not isinstance(value, dict) or value.get('chain_id') != config.chain_id or value.get('denom') != config.native_denom or value.get('wrapped') is not False:
            return decision('defer', 'wrapped_or_wrong_asset_flow')
        try:
            start = utc(value['window_start_utc'])
            end = utc(value['window_end_utc'])
            labelled = utc(value['labels_available_at_utc'])
            amount = float(value['net_inflow_native'])
            valid = (labelled <= start < end <= cutoff and math.isfinite(amount)
                     and value.get('window_complete') is True and value.get('entities_deduped') is True
                     and bool(value.get('venues')))
        except (KeyError, ValueError, TypeError, AttributeError):
            valid = False
        if not valid:
            return decision('defer', 'flow_window_or_historical_labels_unverified')
        if amount <= 0:
            return decision('veto_short', 'specified_venues_lack_positive_native_inflow')
    return decision('pass_to_baseline', 'safety_checks_passed_not_a_trade_signal')
