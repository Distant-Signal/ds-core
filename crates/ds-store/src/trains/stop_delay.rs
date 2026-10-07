//! A train's `delayMinutes` as a passenger sees it: measured against the
//! PUBLIC timetable at the passenger's own stop (design doc §9 decision 2).
//!
//! `train_current_state.delay_minutes` is TRUST's running delay, from the
//! train's latest report anywhere (a pass included), against the working
//! timetable. It stays internal: it is the input to the forecasts here and
//! to the per-stop estimates (`journey::apply_delay_estimates`). Every route
//! that serves a train-level `delayMinutes` replaces it through
//! [`apply_public_delays`] with:
//!
//! * the delay at the passenger's own stop (where they get off: a tracked
//!   train's pin destination, a journey leg's destination), measured once
//!   the train has reported there and forecast before then
//!   (`delayProvisional: true`); or
//! * with no stop of their own (the public train page, a line's trains),
//!   the delay at the latest call the train reported at.
//!
//! See `common::public_delay` for the arithmetic and the fallbacks, and
//! `delayBasis` for which baseline was used.

use sqlx::PgPool;

pub use common::public_delay::db::StopDelayTarget;
pub use common::public_delay::{DelayBasis, StopDelay};

/// [`common::public_delay::db::stop_delays`] with this process's CORPUS
/// crosswalk setting.
pub async fn stop_delays(
    pool: &PgPool,
    targets: &[StopDelayTarget],
) -> anyhow::Result<Vec<Option<StopDelay>>> {
    common::public_delay::db::stop_delays(pool, targets, crate::corpus::fallback_enabled()).await
}

/// A served row carrying a train-level delay.
pub trait PublicDelayFields {
    /// The train and the passenger's stop on it; `None` when the row has no
    /// matched train (its delay is left alone).
    fn delay_target(&self) -> Option<StopDelayTarget>;
    /// Stores the delay at the target's stop (`None`: not known).
    fn set_public_delay(&mut self, delay: Option<StopDelay>);
}

/// Replaces each row's delay with the public one at its own stop, in one
/// batched read for the whole slice (two queries; none when no row has a
/// matched train).
pub async fn apply_public_delays<T: PublicDelayFields>(
    pool: &PgPool,
    rows: &mut [T],
) -> anyhow::Result<()> {
    let mut positions = Vec::new();
    let mut targets = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        if let Some(target) = row.delay_target() {
            positions.push(index);
            targets.push(target);
        }
    }
    if targets.is_empty() {
        return Ok(());
    }
    let delays = stop_delays(pool, &targets).await?;
    for (index, delay) in positions.into_iter().zip(delays) {
        rows[index].set_public_delay(delay);
    }
    Ok(())
}

/// [`StopDelayTarget`] from a row's usual columns: `None` without a matched
/// train (no `trains` row or no UID yet).
pub fn target(
    trains_id: Option<i64>,
    train_uid: Option<&str>,
    service_date: chrono::NaiveDate,
    stop_crs: Option<&str>,
    working_delay_minutes: Option<i32>,
) -> Option<StopDelayTarget> {
    Some(StopDelayTarget {
        trains_id: trains_id?,
        train_uid: train_uid?.to_string(),
        service_date,
        stop_crs: stop_crs
            .map(str::trim)
            .filter(|crs| !crs.is_empty())
            .map(str::to_uppercase),
        working_delay_minutes,
    })
}

/// The three wire fields a delay-carrying row stores, in one place so every
/// row type spells them the same way.
pub fn split(delay: Option<StopDelay>) -> (Option<i32>, Option<DelayBasis>, bool) {
    match delay {
        Some(delay) => (Some(delay.minutes), Some(delay.basis), delay.provisional),
        None => (None, None, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_without_a_matched_train_has_no_target() {
        let date = chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap();
        assert_eq!(
            target(None, Some("C01372"), date, Some("MTH"), Some(3)),
            None
        );
        assert_eq!(target(Some(1), None, date, Some("MTH"), Some(3)), None);
        let target = target(Some(1), Some("C01372"), date, Some(" mth "), Some(3)).unwrap();
        assert_eq!(target.stop_crs.as_deref(), Some("MTH"));
        let blank = super::target(Some(1), Some("C01372"), date, Some(" "), None).unwrap();
        assert_eq!(blank.stop_crs, None);
    }

    #[test]
    fn split_spells_the_three_fields() {
        assert_eq!(split(None), (None, None, false));
        assert_eq!(
            split(Some(StopDelay {
                minutes: 4,
                basis: DelayBasis::PublicSchedule,
                provisional: true
            })),
            (Some(4), Some(DelayBasis::PublicSchedule), true)
        );
    }
}
