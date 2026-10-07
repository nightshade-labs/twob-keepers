use anchor_lang::prelude::Pubkey;
use std::collections::{BTreeSet, HashMap, HashSet};
use tokio::time::{Duration, Instant};
use twob_keepers::twob_anchor::accounts::TradePosition;

use crate::settlement::public_close_slot;

struct Entry {
    position: TradePosition,
    observed_slot: u64,
    next_check: Instant,
    deferred: bool,
}

/// One removable timer per position; updates do not accumulate stale heap entries.
pub struct Schedule {
    entries: HashMap<Pubkey, Entry>,
    deadlines: BTreeSet<(Instant, Pubkey)>,
    // Tombstones stop queued pre-close notifications from resurrecting positions.
    versions: HashMap<Pubkey, u64>,
    snapshot_slot: Option<u64>,
    slot_duration: Duration,
    batch_window: Duration,
}

impl Schedule {
    pub fn new(slot_duration: Duration, batch_window: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            deadlines: BTreeSet::new(),
            versions: HashMap::new(),
            snapshot_slot: None,
            slot_duration,
            batch_window,
        }
    }

    pub fn snapshot_slot(&self) -> Option<u64> {
        self.snapshot_slot
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn next_check(&self) -> Option<Instant> {
        self.deadlines.first().map(|(deadline, _)| *deadline)
    }

    pub fn due(&self, now: Instant, limit: usize) -> Vec<Pubkey> {
        // Collect positions estimated to have ended during the first timer's window.
        // A reconciliation wake can also drain an already-overdue backlog immediately.
        // Explicit retry deadlines are never advanced.
        self.deadlines
            .iter()
            .take_while(|(deadline, _)| *deadline <= now + self.batch_window)
            .filter(|(deadline, address)| *deadline <= now || !self.entries[address].deferred)
            .take(limit)
            .map(|(_, address)| *address)
            .collect()
    }

    pub fn notification(
        &mut self,
        address: Pubkey,
        position: Option<TradePosition>,
        slot: u64,
        now: Instant,
    ) {
        // The snapshot is a complete state at this slot, including deletions.
        if self.snapshot_slot.is_some_and(|snapshot| slot <= snapshot) {
            return;
        }
        self.update(address, position, slot, now, None);
    }

    pub fn reconcile(&mut self, positions: Vec<(Pubkey, TradePosition)>, slot: u64, now: Instant) {
        if self.snapshot_slot.is_some_and(|snapshot| slot < snapshot) {
            return;
        }
        let present: HashSet<_> = positions.iter().map(|(address, _)| *address).collect();
        let removed: Vec<_> = self
            .entries
            .iter()
            .filter(|(address, entry)| entry.observed_slot <= slot && !present.contains(address))
            .map(|(address, _)| *address)
            .collect();
        for address in removed {
            self.remove(&address);
        }
        for (address, position) in positions {
            self.update(address, Some(position), slot, now, None);
        }
        self.snapshot_slot = Some(slot);
        self.versions.retain(|_, observed| *observed > slot);
    }

    /// Fresh HTTP outcomes explicitly reset the timer (including slow negative-cache retries).
    pub fn outcome(
        &mut self,
        address: Pubkey,
        position: Option<TradePosition>,
        slot: u64,
        now: Instant,
        retry: Option<Duration>,
    ) {
        if self.snapshot_slot.is_some_and(|snapshot| slot < snapshot) {
            return;
        }
        self.update(address, position, slot, now, retry);
        if retry.is_none() {
            // An unchanged position can still be not due if slots ran slower than estimated.
            if let Some(entry) = self.entries.get(&address) {
                let position = entry.position;
                if entry.observed_slot <= slot {
                    let deadline = self.deadline(&position, slot, now);
                    self.reschedule(address, deadline);
                    self.entries.get_mut(&address).unwrap().deferred = false;
                }
            }
        }
    }

    pub fn retry(&mut self, address: Pubkey, now: Instant, delay: Duration) {
        self.reschedule(address, now + delay);
        if let Some(entry) = self.entries.get_mut(&address) {
            entry.deferred = true;
        }
    }

    fn update(
        &mut self,
        address: Pubkey,
        position: Option<TradePosition>,
        slot: u64,
        now: Instant,
        retry: Option<Duration>,
    ) {
        if self.snapshot_slot.is_some_and(|snapshot| slot < snapshot) {
            return;
        }
        let newest = self
            .versions
            .get(&address)
            .copied()
            .into_iter()
            .chain(self.entries.get(&address).map(|entry| entry.observed_slot))
            .max();
        if newest.is_some_and(|observed| slot < observed) {
            return;
        }
        self.versions.insert(address, slot);
        let Some(position) = position else {
            self.remove(&address);
            return;
        };
        let estimated_deadline = self.deadline(&position, slot, now);
        if let Some(entry) = self.entries.get_mut(&address) {
            if bytemuck::bytes_of(&entry.position) == bytemuck::bytes_of(&position) {
                entry.observed_slot = slot;
                if let Some(delay) = retry {
                    entry.deferred = true;
                    self.reschedule(address, now + delay);
                } else if !entry.deferred {
                    // Reconciliation can discover that the chain ran faster than estimated.
                    // Never move a due timer back just because identical data was delivered.
                    let deadline = entry.next_check.min(estimated_deadline);
                    self.reschedule(address, deadline);
                }
                return;
            }
        }
        let next_check = retry.map(|delay| now + delay).unwrap_or(estimated_deadline);
        self.remove(&address);
        self.entries.insert(
            address,
            Entry {
                position,
                observed_slot: slot,
                next_check,
                deferred: retry.is_some(),
            },
        );
        self.deadlines.insert((next_check, address));
    }

    fn deadline(&self, position: &TradePosition, slot: u64, now: Instant) -> Instant {
        let slots = public_close_slot(position)
            .unwrap_or(u64::MAX)
            .saturating_sub(slot);
        // Cap estimates to one day; this also bounds malformed/overflowing slot arithmetic.
        let millis = (u128::from(slots) * self.slot_duration.as_millis()).min(86_400_000) as u64;
        now + Duration::from_millis(millis) + self.batch_window
    }

    fn reschedule(&mut self, address: Pubkey, deadline: Instant) {
        if let Some(entry) = self.entries.get_mut(&address) {
            self.deadlines.remove(&(entry.next_check, address));
            entry.next_check = deadline;
            self.deadlines.insert((deadline, address));
        }
    }

    fn remove(&mut self, address: &Pubkey) {
        if let Some(entry) = self.entries.remove(address) {
            self.deadlines.remove(&(entry.next_check, *address));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use twob_keepers::MAXIMUM_DURATION_SLOTS;

    fn position(end: u64) -> TradePosition {
        TradePosition {
            last_update_slot: end,
            ..Default::default()
        }
    }

    fn schedule() -> Schedule {
        Schedule::new(Duration::from_millis(400), Duration::from_secs(5))
    }

    #[test]
    fn sleeps_until_end_plus_batch_window_and_accepts_earlier_creation() {
        let mut queue = schedule();
        let now = Instant::now();
        let later = Pubkey::new_unique();
        let earlier = Pubkey::new_unique();
        queue.reconcile(vec![(later, position(200))], 100, now);
        assert_eq!(queue.next_check(), Some(now + Duration::from_secs(45)));
        queue.notification(earlier, Some(position(110)), 101, now);
        assert_eq!(queue.next_check(), Some(now + Duration::from_millis(8600)));
        assert!(queue.due(now + Duration::from_secs(3), 32).is_empty());
        assert_eq!(queue.due(now + Duration::from_secs(9), 32), vec![earlier]);
    }

    #[test]
    fn pause_resume_and_receiver_changes_replace_one_timer() {
        let mut queue = schedule();
        let now = Instant::now();
        let address = Pubkey::new_unique();
        let mut value = position(110);
        queue.notification(address, Some(value), 100, now);
        value.start_slot = 1;
        value.paused_at_slot = 101;
        queue.notification(address, Some(value), 101, now);
        assert!(queue.due(now + Duration::from_secs(60), 32).is_empty());
        assert_eq!(public_close_slot(&value), Some(MAXIMUM_DURATION_SLOTS + 2));
        value.paused_at_slot = 0;
        value.last_update_slot = 115;
        queue.notification(address, Some(value), 102, now);
        assert_eq!(queue.deadlines.len(), 1);
        assert_eq!(queue.next_check(), Some(now + Duration::from_millis(10200)));
    }

    #[test]
    fn missing_receivers_do_not_spin_or_starve_other_positions() {
        let mut queue = schedule();
        let now = Instant::now();
        let missing = Pubkey::new_unique();
        let ready = Pubkey::new_unique();
        queue.reconcile(
            vec![(missing, position(90)), (ready, position(90))],
            100,
            now,
        );
        queue.outcome(
            missing,
            Some(position(90)),
            101,
            now,
            Some(Duration::from_secs(300)),
        );
        queue.reconcile(
            vec![(missing, position(90)), (ready, position(90))],
            102,
            now,
        );
        assert_eq!(queue.due(now + Duration::from_secs(6), 32), vec![ready]);
        let mut changed = position(90);
        changed.base_receiver = Pubkey::new_unique();
        queue.notification(missing, Some(changed), 103, now);
        assert_eq!(queue.due(now + Duration::from_secs(6), 32).len(), 2);
    }

    #[test]
    fn snapshot_and_notification_races_preserve_newer_state_and_deletions() {
        let mut queue = schedule();
        let now = Instant::now();
        let newer = Pubkey::new_unique();
        let closed = Pubkey::new_unique();
        queue.notification(newer, Some(position(150)), 110, now);
        queue.notification(closed, Some(position(120)), 90, now);
        queue.reconcile(vec![], 100, now);
        queue.notification(closed, Some(position(120)), 99, now);
        queue.notification(newer, Some(position(120)), 109, now);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.entries[&newer].position.last_update_slot, 150);
        queue.outcome(newer, None, 111, now, None);
        queue.notification(newer, Some(position(150)), 110, now);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn slow_slots_reschedule_unchanged_positions_after_fresh_http_read() {
        let mut queue = schedule();
        let now = Instant::now();
        let address = Pubkey::new_unique();
        queue.reconcile(vec![(address, position(110))], 100, now);
        let later = now + Duration::from_secs(10);
        queue.outcome(address, Some(position(110)), 105, later, None);
        assert_eq!(queue.next_check(), Some(later + Duration::from_secs(7)));
        assert!(queue.due(later, 32).is_empty());
    }

    #[test]
    fn faster_chain_brings_future_checks_forward_without_resetting_backoff() {
        let mut queue = schedule();
        let now = Instant::now();
        let address = Pubkey::new_unique();
        queue.reconcile(vec![(address, position(1000))], 100, now);
        assert_eq!(queue.next_check(), Some(now + Duration::from_secs(365)));
        queue.reconcile(
            vec![(address, position(1000))],
            900,
            now + Duration::from_secs(10),
        );
        assert_eq!(queue.next_check(), Some(now + Duration::from_secs(55)));
        queue.retry(address, now, Duration::from_secs(300));
        queue.reconcile(
            vec![(address, position(1000))],
            1001,
            now + Duration::from_secs(20),
        );
        assert_eq!(queue.next_check(), Some(now + Duration::from_secs(300)));
    }

    #[test]
    fn stale_outcomes_cannot_restore_a_deleted_position_or_defer_newer_changes() {
        let mut queue = schedule();
        let now = Instant::now();
        let address = Pubkey::new_unique();
        queue.reconcile(vec![], 100, now);
        queue.outcome(address, Some(position(90)), 99, now, None);
        assert_eq!(queue.len(), 0);
        queue.notification(address, Some(position(110)), 101, now);
        let deadline = queue.next_check();
        queue.outcome(
            address,
            Some(position(90)),
            100,
            now,
            Some(Duration::from_secs(300)),
        );
        assert_eq!(queue.next_check(), deadline);
        queue.outcome(address, None, 100, now, None);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn nearby_endings_share_the_first_window_but_retries_never_run_early() {
        let mut queue = schedule();
        let now = Instant::now();
        let first = Pubkey::new_unique();
        let second = Pubkey::new_unique();
        let retry = Pubkey::new_unique();
        queue.reconcile(
            vec![
                (first, position(100)),
                (second, position(110)),
                (retry, position(90)),
            ],
            100,
            now,
        );
        queue.retry(retry, now, Duration::from_secs(9));
        assert_eq!(queue.next_check(), Some(now + Duration::from_secs(5)));
        let batch = queue.due(now + Duration::from_secs(5), 32);
        assert_eq!(batch.len(), 2);
        assert!(batch.contains(&first));
        assert!(batch.contains(&second));
        assert!(!batch.contains(&retry));
    }
}
