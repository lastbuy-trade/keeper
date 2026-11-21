use solana_pubkey::Pubkey;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// A validated discovery hint. Execution must refresh the curve at or beyond `slot`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub curve: Pubkey,
    pub mint: Pubkey,
    pub slot: u64,
    /// Stream write version, or None for a snapshot covering the complete slot.
    pub write_version: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnqueueResult {
    Added,
    Coalesced,
    Stale,
    Deferred,
    Overflow,
}

struct RetryHistory {
    ready_at: Instant,
    failures: u32,
    order: u64,
}

struct Entry {
    candidate: Candidate,
    order: u64,
    ready_at: Instant,
    failures: u32,
    dirty: bool,
}

/// Bounded per-curve work queue. The active entry occupies capacity until released.
/// Inclusion ambiguity must keep the lane occupied, including across recovery.
pub struct Scheduler {
    capacity: usize,
    entries: HashMap<Pubkey, Entry>,
    active: Option<Pubkey>,
    sequence: u64,
    overflow_generation: u64,
    reconciled_generation: u64,
    retry_base: Duration,
    retry_max: Duration,
    max_failures: u8,
    history: HashMap<Pubkey, RetryHistory>,
    exhausted: std::collections::HashSet<Pubkey>,
    finalized_success: std::collections::HashSet<Pubkey>,
    // Once retry history is lost, pace all new admissions to bound forgotten retries.
    forgotten_until: Option<Instant>,
    admission_cursor: Option<Pubkey>,
    scan_cursor: Option<Pubkey>,
    scan_generation: Option<u64>,
    scan_page_generation: u64,
    scan_incomplete: bool,
}

impl Scheduler {
    pub fn new(capacity: usize, retry_base: Duration, retry_max: Duration) -> Self {
        Self::new_with_failure_limit(capacity, retry_base, retry_max, 3)
    }

    pub fn new_with_failure_limit(
        capacity: usize,
        retry_base: Duration,
        retry_max: Duration,
        max_failures: u8,
    ) -> Self {
        assert!(
            capacity > 0 && !retry_base.is_zero() && retry_max >= retry_base && max_failures > 0
        );
        Self {
            capacity,
            entries: HashMap::new(),
            active: None,
            sequence: 0,
            overflow_generation: 0,
            reconciled_generation: 0,
            retry_base,
            retry_max,
            max_failures,
            history: HashMap::new(),
            exhausted: std::collections::HashSet::new(),
            finalized_success: std::collections::HashSet::new(),
            forgotten_until: None,
            admission_cursor: None,
            scan_cursor: None,
            scan_generation: None,
            scan_page_generation: 0,
            scan_incomplete: false,
        }
    }

    pub fn enqueue(&mut self, candidate: Candidate, now: Instant) -> EnqueueResult {
        if self.exhausted.contains(&candidate.curve)
            || self.finalized_success.contains(&candidate.curve)
        {
            return EnqueueResult::Stale;
        }
        if let Some(entry) = self.entries.get_mut(&candidate.curve) {
            let old = entry.candidate;
            let newer = candidate.slot > old.slot
                || (candidate.slot == old.slot
                    && match (old.write_version, candidate.write_version) {
                        (Some(a), Some(b)) => b > a,
                        (Some(_), None) => true,
                        _ => false,
                    });
            if !newer {
                return EnqueueResult::Stale;
            }
            entry.candidate = candidate;
            if self.active == Some(candidate.curve) {
                entry.dirty = true;
            }
            return EnqueueResult::Coalesced;
        }
        let rate_limited = self.forgotten_until.is_some_and(|until| now < until);
        if self.admission_cursor == Some(candidate.curve)
            || self
                .history
                .get(&candidate.curve)
                .is_some_and(|h| now < h.ready_at)
            || rate_limited
        {
            self.overflow_generation = self.overflow_generation.saturating_add(1);
            return EnqueueResult::Deferred;
        }
        if self.entries.len() == self.capacity {
            self.overflow_generation = self.overflow_generation.saturating_add(1);
            // A failed, inactive item must not monopolize finite capacity.
            let victim = self
                .entries
                .iter()
                .filter(|(key, entry)| self.active != Some(**key) && entry.failures > 0)
                .min_by_key(|(_, entry)| entry.order)
                .map(|(key, _)| *key);
            if let Some(victim) = victim {
                self.evict(victim, now);
            } else {
                return EnqueueResult::Overflow;
            }
        }
        self.sequence = self.sequence.saturating_add(1);
        if self.forgotten_until.is_some() {
            self.forgotten_until = Some(now.checked_add(self.retry_base).unwrap_or(now));
        }
        if self
            .admission_cursor
            .is_some_and(|key| key != candidate.curve)
        {
            self.admission_cursor = None;
        }
        let failures = self
            .history
            .remove(&candidate.curve)
            .map_or(0, |h| h.failures);
        self.entries.insert(
            candidate.curve,
            Entry {
                candidate,
                order: self.sequence,
                ready_at: now,
                failures,
                dirty: false,
            },
        );
        EnqueueResult::Added
    }

    /// Returns one candidate and reserves the single submission lane.
    pub fn next(&mut self, now: Instant) -> Option<Candidate> {
        if self.active.is_some() {
            return None;
        }
        let entry = self
            .entries
            .values()
            .filter(|entry| entry.ready_at <= now)
            .min_by_key(|entry| entry.order)?;
        let candidate = entry.candidate;
        self.active = Some(candidate.curve);
        Some(candidate)
    }

    pub fn active(&self) -> Option<Pubkey> {
        self.active
    }
    pub fn contains(&self, curve: &Pubkey) -> bool {
        self.entries.contains_key(curve)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn reconciliation_needed(&self) -> bool {
        self.overflow_generation != self.reconciled_generation
    }
    /// Capture before scanning; completing an older scan cannot hide a later overflow.
    pub fn reconciliation_token(&self) -> u64 {
        self.overflow_generation
    }

    /// Start a scan over candidates sorted by curve address. Resume inclusively at
    /// this address; do not offer earlier addresses until the next full pass.
    pub fn scan_begin(&mut self) -> Option<Pubkey> {
        self.scan_page_generation = self.overflow_generation;
        match self.scan_generation {
            Some(generation) if generation != self.overflow_generation => {
                self.scan_incomplete = true;
            }
            None => self.scan_generation = Some(self.overflow_generation),
            _ => {}
        }
        self.scan_cursor
    }

    /// Offer candidates in ascending curve order. Stop the scan on Overflow;
    /// the next scan resumes at that candidate. Deferred entries are skipped
    /// for this pass, while retaining the reconciliation obligation.
    pub fn scan_enqueue(&mut self, candidate: Candidate, now: Instant) -> EnqueueResult {
        if self
            .scan_cursor
            .is_some_and(|cursor| candidate.curve < cursor)
        {
            return EnqueueResult::Deferred;
        }
        if self.scan_generation != Some(self.overflow_generation) {
            self.scan_incomplete = true;
        }
        let before = self.overflow_generation;
        let rate_limited = self.forgotten_until.is_some_and(|until| now < until);
        let result = self.enqueue(candidate, now);
        if result == EnqueueResult::Deferred && rate_limited {
            self.scan_cursor = Some(candidate.curve);
            self.scan_generation = Some(self.overflow_generation);
            return EnqueueResult::Overflow;
        }
        if result == EnqueueResult::Deferred
            || (result != EnqueueResult::Overflow && self.overflow_generation != before)
        {
            self.scan_incomplete = true;
        }
        self.scan_generation = Some(self.overflow_generation);
        if result == EnqueueResult::Overflow {
            self.scan_cursor = Some(candidate.curve);
        }
        result
    }

    /// Call once after each scan. A partial pass never acknowledges coverage.
    /// Pages collectively cover one pass; outside overflow or deferred work
    /// requires another pass even after the end is reached.
    pub fn scan_finished(&mut self, token: u64, exhausted: bool) {
        if token != self.scan_page_generation {
            self.scan_incomplete = true;
        }
        if !exhausted {
            return;
        }
        self.scan_cursor = None;
        if self.scan_generation != Some(self.overflow_generation) {
            self.scan_incomplete = true;
        }
        if !self.scan_incomplete {
            self.reconciliation_completed(self.overflow_generation);
        } else if self.admission_cursor.take().is_some() {
            self.overflow_generation = self.overflow_generation.saturating_add(1);
        }
        self.scan_generation = None;
        self.scan_incomplete = false;
    }
    pub fn reconciliation_completed(&mut self, token: u64) {
        // If a complete scan found no alternative, permit the skipped curve on
        // a subsequent scan, subject to its retained cooldown.
        if self.admission_cursor.take().is_some() {
            self.overflow_generation = self.overflow_generation.saturating_add(1);
            return;
        }
        if token == self.overflow_generation {
            self.reconciled_generation = token;
        }
    }

    /// Release only after a finalized outcome or a decision that no submission exists.
    /// A later notification received while active is retained for fresh validation.
    pub fn complete(&mut self, curve: Pubkey, now: Instant) -> bool {
        if self.active != Some(curve) {
            return false;
        }
        self.active = None;
        self.history.remove(&curve);
        if !self.finalized_success.contains(&curve)
            && self.entries.get(&curve).is_some_and(|entry| entry.dirty)
        {
            if let Some(entry) = self.entries.get_mut(&curve) {
                entry.dirty = false;
                entry.failures = 0;
                entry.ready_at = now;
            }
        } else {
            self.entries.remove(&curve);
        }
        true
    }

    /// Release the lane after an RPC or recovery delay without counting a curve failure.
    pub fn defer(&mut self, curve: Pubkey, now: Instant) {
        if self.active == Some(curve) {
            self.active = None;
            if self.finalized_success.contains(&curve) {
                self.entries.remove(&curve);
            } else if let Some(entry) = self.entries.get_mut(&curve) {
                entry.ready_at = now.checked_add(self.retry_max).unwrap_or(now);
            }
        }
    }

    /// Retain a failed candidate with exponential cooldown capped at `retry_max`.
    /// Evict on overflow or when the configured failure limit is reached.
    pub fn retry(&mut self, curve: Pubkey, now: Instant) -> bool {
        if self.active != Some(curve) {
            return false;
        }
        self.active = None;
        if self.finalized_success.contains(&curve) {
            self.entries.remove(&curve);
            return true;
        }
        let evict = self.reconciliation_needed()
            || self.entries.get(&curve).is_some_and(|entry| {
                entry.failures.saturating_add(1) >= u32::from(self.max_failures)
            });
        if let Some(entry) = self.entries.get_mut(&curve) {
            let multiplier = 1u32.checked_shl(entry.failures.min(31)).unwrap_or(u32::MAX);
            let delay = self
                .retry_base
                .saturating_mul(multiplier)
                .min(self.retry_max);
            entry.ready_at = now.checked_add(delay).unwrap_or(now);
            entry.failures = entry.failures.saturating_add(1);
            entry.dirty = false;
        }
        if evict {
            if self
                .entries
                .get(&curve)
                .is_some_and(|entry| entry.failures >= u32::from(self.max_failures))
            {
                self.exhausted.insert(curve);
            }
            self.evict(curve, now);
            self.overflow_generation = self.overflow_generation.saturating_add(1);
        }
        true
    }

    pub fn restore_exhausted(&mut self, curves: impl IntoIterator<Item = Pubkey>) {
        self.exhausted.extend(curves);
    }

    /// Permanently evict a curve after finalized success, including future admissions.
    pub fn finalize_success(&mut self, curve: Pubkey) {
        self.finalized_success.insert(curve);
        if self.active != Some(curve) {
            self.entries.remove(&curve);
        } else if let Some(entry) = self.entries.get_mut(&curve) {
            entry.dirty = false;
        }
        self.history.remove(&curve);
        self.exhausted.remove(&curve);
    }

    pub fn exhausted(&self, curve: &Pubkey) -> bool {
        self.exhausted.contains(curve)
    }

    fn evict(&mut self, curve: Pubkey, now: Instant) {
        if let Some(entry) = self.entries.remove(&curve) {
            if self.history.len() == self.capacity
                && !self.history.contains_key(&curve)
                && let Some(oldest) = self
                    .history
                    .iter()
                    .min_by_key(|(_, h)| h.order)
                    .map(|(key, _)| *key)
                && let Some(history) = self.history.remove(&oldest)
            {
                self.forgotten_until.get_or_insert(
                    history
                        .ready_at
                        .max(now.checked_add(self.retry_base).unwrap_or(now)),
                );
            }
            self.history.insert(
                curve,
                RetryHistory {
                    ready_at: entry.ready_at,
                    failures: entry.failures,
                    order: self.sequence,
                },
            );
            self.admission_cursor = Some(curve);
        }
    }

    /// Remove a candidate after a fresh read proves it ineligible. Active work must be resolved.
    pub fn remove(&mut self, curve: Pubkey) -> bool {
        if self.active == Some(curve) {
            return false;
        }
        self.entries.remove(&curve).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn candidate(curve: Pubkey, slot: u64) -> Candidate {
        Candidate {
            curve,
            mint: Pubkey::new_unique(),
            slot,
            write_version: Some(0),
        }
    }
    fn scheduler(capacity: usize) -> Scheduler {
        Scheduler::new(capacity, Duration::from_secs(2), Duration::from_secs(5))
    }
    fn distinct_candidates() -> Vec<Candidate> {
        let mut curves: Vec<_> = (0..3).map(|_| candidate(Pubkey::new_unique(), 1)).collect();
        curves.sort_by_key(|candidate| candidate.curve);
        curves
    }

    #[test]
    fn coalesces_and_keeps_updates_during_active_work() {
        let now = Instant::now();
        let key = Pubkey::new_unique();
        let mut s = scheduler(1);
        assert_eq!(s.enqueue(candidate(key, 2), now), EnqueueResult::Added);
        assert_eq!(s.enqueue(candidate(key, 1), now), EnqueueResult::Stale);
        assert_eq!(s.next(now).unwrap().slot, 2);
        assert_eq!(s.enqueue(candidate(key, 3), now), EnqueueResult::Coalesced);
        assert!(s.next(now).is_none());
        assert!(s.complete(key, now));
        assert_eq!(s.next(now).unwrap().slot, 3);
        assert!(s.complete(key, now));
        assert!(s.is_empty());
    }
    #[test]
    fn finalized_success_suppresses_admissions_and_releases_matching_active() {
        let now = Instant::now();
        let mut s = scheduler(2);
        let candidates = distinct_candidates();
        let completed = candidates[0];
        let other = candidates[1];
        assert_eq!(s.enqueue(completed, now), EnqueueResult::Added);
        assert_eq!(s.enqueue(other, now), EnqueueResult::Added);
        assert_eq!(s.next(now).unwrap().curve, completed.curve);
        s.finalize_success(completed.curve);
        assert_eq!(s.active(), Some(completed.curve));
        assert!(s.complete(completed.curve, now));
        assert_eq!(s.active(), None);
        assert_eq!(s.len(), 1);
        assert_eq!(
            s.enqueue(candidate(completed.curve, 99), now),
            EnqueueResult::Stale
        );
        assert_eq!(s.next(now).unwrap().curve, other.curve);
        assert!(s.complete(other.curve, now));
        assert!(s.is_empty());
    }
    #[test]
    fn overflow_requires_a_scan_that_covers_latest_overflow() {
        let now = Instant::now();
        let mut s = scheduler(1);
        s.enqueue(candidate(Pubkey::new_unique(), 1), now);
        assert_eq!(
            s.enqueue(candidate(Pubkey::new_unique(), 1), now),
            EnqueueResult::Overflow
        );
        let token = s.reconciliation_token();
        s.enqueue(candidate(Pubkey::new_unique(), 1), now);
        s.reconciliation_completed(token);
        assert!(s.reconciliation_needed());
        s.reconciliation_completed(s.reconciliation_token());
        assert!(!s.reconciliation_needed());
    }
    #[test]
    fn lane_and_capped_cooldown() {
        let now = Instant::now();
        let key = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let mut s = scheduler(2);
        s.enqueue(candidate(key, 1), now);
        s.enqueue(candidate(other, 1), now);
        assert_eq!(s.next(now).unwrap().curve, key);
        assert!(s.next(now).is_none());
        assert!(!s.complete(other, now));
        assert!(s.retry(key, now));
        assert_eq!(s.next(now).unwrap().curve, other);
        assert!(s.complete(other, now));
        assert!(s.next(now + Duration::from_secs(1)).is_none());
        let t = now + Duration::from_secs(2);
        assert_eq!(s.next(t).unwrap().curve, key);
        s.retry(key, t);
        assert!(s.next(t + Duration::from_secs(3)).is_none());
        let t = t + Duration::from_secs(4);
        s.next(t).unwrap();
        s.retry(key, t);
        assert!(s.is_empty());
        assert!(s.reconciliation_needed());
    }

    #[test]
    fn duplicate_active_update_does_not_requeue_but_new_write_does() {
        let now = Instant::now();
        let mut s = scheduler(1);
        let c = candidate(Pubkey::new_unique(), 5);
        s.enqueue(c, now);
        s.next(now);
        assert_eq!(s.enqueue(c, now), EnqueueResult::Stale);
        s.complete(c.curve, now);
        assert!(s.is_empty());
        s.enqueue(c, now);
        s.next(now);
        let newer = Candidate {
            write_version: Some(1),
            ..c
        };
        assert_eq!(s.enqueue(newer, now), EnqueueResult::Coalesced);
        s.complete(c.curve, now);
        assert_eq!(s.next(now), Some(newer));
    }

    #[test]
    fn capacity_one_failing_work_yields_to_overflow() {
        let now = Instant::now();
        let mut s = scheduler(1);
        let failing = candidate(Pubkey::new_unique(), 1);
        let waiting = candidate(Pubkey::new_unique(), 1);
        s.enqueue(failing, now);
        s.next(now);
        assert_eq!(s.enqueue(waiting, now), EnqueueResult::Overflow);
        s.retry(failing.curve, now);
        assert!(s.is_empty());
        assert!(s.reconciliation_needed());
        assert_eq!(s.enqueue(waiting, now), EnqueueResult::Added);
        assert_eq!(s.next(now), Some(waiting));
        s.complete(waiting.curve, now);
        assert_eq!(s.enqueue(failing, now), EnqueueResult::Deferred);
        assert!(s.reconciliation_needed());
    }

    #[test]
    fn repeated_fixed_order_scan_admits_second_curve_and_preserves_cooldown() {
        let now = Instant::now();
        let mut s = scheduler(1);
        let a = candidate(Pubkey::new_unique(), 1);
        let b = candidate(Pubkey::new_unique(), 1);
        for c in [a, b] {
            s.enqueue(c, now);
        }
        assert_eq!(s.next(now), Some(a));
        s.retry(a.curve, now);
        for c in [a, b] {
            s.enqueue(c, now);
        }
        assert_eq!(s.next(now), Some(b));
        s.complete(b.curve, now);
        assert_eq!(s.enqueue(a, now), EnqueueResult::Deferred);
        assert_eq!(
            s.enqueue(a, now + Duration::from_secs(2)),
            EnqueueResult::Added
        );
        assert_eq!(s.next(now + Duration::from_secs(2)), Some(a));
        s.retry(a.curve, now + Duration::from_secs(2));
        let token = s.reconciliation_token();
        s.reconciliation_completed(token);
        assert_eq!(
            s.enqueue(a, now + Duration::from_secs(3)),
            EnqueueResult::Deferred
        );
        assert_eq!(
            s.enqueue(a, now + Duration::from_secs(6)),
            EnqueueResult::Added
        );
    }

    #[test]
    fn resumed_scans_reach_third_curve_after_repeated_failures() {
        let now = Instant::now();
        let mut s = scheduler(1);
        let curves = distinct_candidates();

        for (index, expected) in curves.iter().enumerate() {
            let now = now + Duration::from_secs(if index == 2 { 2 } else { 0 });
            let token = s.reconciliation_token();
            assert_eq!(s.scan_begin(), index.checked_sub(1).map(|_| expected.curve));
            let mut exhausted = true;
            for candidate in &curves {
                if s.scan_enqueue(*candidate, now) == EnqueueResult::Overflow {
                    exhausted = false;
                    break;
                }
            }
            s.scan_finished(token, exhausted);
            assert_eq!(s.next(now), Some(*expected));
            if index < 2 {
                assert!(s.retry(expected.curve, now));
            }
        }

        assert!(s.complete(curves[2].curve, now + Duration::from_secs(2)));
        assert_eq!(
            s.enqueue(curves[1], now + Duration::from_secs(2)),
            EnqueueResult::Deferred
        );
        assert_eq!(
            s.enqueue(curves[1], now + Duration::from_secs(4)),
            EnqueueResult::Added
        );
    }

    #[test]
    fn forgotten_failures_cannot_retry_immediately_after_wraparound() {
        let now = Instant::now();
        let mut s = scheduler(1);
        let curves = distinct_candidates();
        for (index, expected) in curves.iter().enumerate() {
            let now = now + Duration::from_secs(if index == 2 { 2 } else { 0 });
            let token = s.reconciliation_token();
            s.scan_begin();
            let mut exhausted = true;
            for candidate in &curves {
                if s.scan_enqueue(*candidate, now) == EnqueueResult::Overflow {
                    exhausted = false;
                    break;
                }
            }
            s.scan_finished(token, exhausted);
            assert_eq!(s.next(now), Some(*expected));
            assert!(s.retry(expected.curve, now));
        }
        assert_eq!(
            s.enqueue(curves[0], now + Duration::from_secs(2)),
            EnqueueResult::Deferred
        );
        assert_eq!(
            s.enqueue(curves[0], now + Duration::from_secs(4)),
            EnqueueResult::Added
        );
    }

    #[test]
    fn paged_scan_acknowledges_coverage_after_processing_each_page() {
        let now = Instant::now();
        let mut s = scheduler(1);
        let curves = distinct_candidates();
        assert_eq!(s.enqueue(curves[0], now), EnqueueResult::Added);
        assert_eq!(s.enqueue(curves[1], now), EnqueueResult::Overflow);
        assert_eq!(s.next(now), Some(curves[0]));
        assert!(s.complete(curves[0].curve, now));

        for (index, expected) in curves.iter().enumerate() {
            let token = s.reconciliation_token();
            s.scan_begin();
            let mut exhausted = true;
            for candidate in &curves {
                if s.scan_enqueue(*candidate, now) == EnqueueResult::Overflow {
                    exhausted = false;
                    break;
                }
            }
            s.scan_finished(token, exhausted);
            assert_eq!(s.next(now), Some(*expected));
            assert!(s.complete(expected.curve, now));
            assert_eq!(s.reconciliation_needed(), index + 1 < curves.len());
        }
    }

    #[test]
    fn paced_admissions_progress_during_sustained_evictions() {
        let now = Instant::now();
        let mut s = scheduler(1);
        s.forgotten_until = Some(now + Duration::from_secs(2));

        for second in [2, 4, 6] {
            let fresh = candidate(Pubkey::new_unique(), 1);
            assert_eq!(
                s.enqueue(fresh, now + Duration::from_secs(second - 1)),
                EnqueueResult::Deferred
            );
            // Simulate ongoing evictions with later individual cooldowns.
            let evicted = candidate(Pubkey::new_unique(), 1);
            s.entries.insert(
                evicted.curve,
                Entry {
                    candidate: evicted,
                    order: 0,
                    ready_at: now + Duration::from_secs(second + 5),
                    failures: 1,
                    dirty: false,
                },
            );
            s.evict(evicted.curve, now + Duration::from_secs(second));
            let at = now + Duration::from_secs(second);
            assert_eq!(s.enqueue(fresh, at), EnqueueResult::Added);
            assert_eq!(s.next(at), Some(fresh));
            assert!(s.complete(fresh.curve, at));
        }
    }

    #[test]
    fn pacing_allows_a_waiter_then_defers_other_candidates() {
        let now = Instant::now();
        let mut s = scheduler(1);
        s.forgotten_until = Some(now + Duration::from_secs(2));
        let waiting = candidate(Pubkey::new_unique(), 1);
        assert_eq!(
            s.enqueue(waiting, now + Duration::from_secs(1)),
            EnqueueResult::Deferred
        );
        let forgotten = candidate(Pubkey::new_unique(), 1);
        s.history.insert(
            forgotten.curve,
            RetryHistory {
                ready_at: now + Duration::from_secs(4),
                failures: 1,
                order: 0,
            },
        );
        let evicted = candidate(Pubkey::new_unique(), 1);
        s.entries.insert(
            evicted.curve,
            Entry {
                candidate: evicted,
                order: 1,
                ready_at: now + Duration::from_secs(4),
                failures: 1,
                dirty: false,
            },
        );
        let at = now + Duration::from_secs(2);
        s.evict(evicted.curve, at);
        assert_eq!(s.enqueue(waiting, at), EnqueueResult::Added);
        assert_eq!(s.enqueue(forgotten, at), EnqueueResult::Deferred);
    }

    #[test]
    fn admission_after_idle_refreshes_expired_limiter() {
        let now = Instant::now();
        let mut s = scheduler(1);
        let old = candidate(Pubkey::new_unique(), 1);
        s.history.insert(
            old.curve,
            RetryHistory {
                ready_at: now + Duration::from_secs(2),
                failures: 1,
                order: 0,
            },
        );
        s.forgotten_until = Some(now + Duration::from_secs(2));
        let at = now + Duration::from_secs(100);
        let current = candidate(Pubkey::new_unique(), 1);
        assert_eq!(s.enqueue(current, at), EnqueueResult::Added);
        assert_eq!(s.next(at), Some(current));
        assert!(s.retry(current.curve, at));
        s.evict(current.curve, at);
        assert_eq!(s.enqueue(old, at), EnqueueResult::Deferred);
        assert_eq!(
            s.enqueue(old, at + Duration::from_secs(2)),
            EnqueueResult::Added
        );
    }

    #[test]
    fn rate_limited_scan_resumes_at_deferred_candidate() {
        let now = Instant::now();
        let mut s = scheduler(1);
        s.forgotten_until = Some(now);
        let curves = distinct_candidates();
        let first = curves[0];
        let second = curves[1];
        for (at, expected) in [(now, first), (now + Duration::from_secs(2), second)] {
            let token = s.reconciliation_token();
            s.scan_begin();
            let mut exhausted = true;
            for candidate in [first, second] {
                if s.scan_enqueue(candidate, at) == EnqueueResult::Overflow {
                    exhausted = false;
                    break;
                }
            }
            s.scan_finished(token, exhausted);
            assert_eq!(s.next(at), Some(expected));
            assert!(s.complete(expected.curve, at));
        }
    }

    #[test]
    fn vanished_waiter_does_not_block_later_candidates() {
        let now = Instant::now();
        let mut s = scheduler(1);
        s.forgotten_until = Some(now + Duration::from_secs(2));
        let curves = distinct_candidates();
        let token = s.reconciliation_token();
        s.scan_begin();
        assert_eq!(s.scan_enqueue(curves[0], now), EnqueueResult::Overflow);
        s.scan_finished(token, false);
        for (index, at) in [(1, 2), (2, 4)] {
            let time = now + Duration::from_secs(at);
            let token = s.reconciliation_token();
            s.scan_begin();
            let mut exhausted = true;
            for candidate in &curves[1..] {
                if s.scan_enqueue(*candidate, time) == EnqueueResult::Overflow {
                    exhausted = false;
                    break;
                }
            }
            s.scan_finished(token, exhausted);
            assert_eq!(s.next(time), Some(curves[index]));
            assert!(s.complete(curves[index].curve, time));
        }
    }
}
