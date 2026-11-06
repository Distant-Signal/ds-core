//! UK bank holidays, read by the api and the notifier.
//!
//! Regular journeys skip a bank holiday in the owner's division when the
//! template says so (distant-signal's
//! docs/superpowers/specs/2026-11-06-regular-journeys-design.md §8,
//! decision 8). The api's `nextDate` and the notifier's evening mint both
//! decide it with [`bank_holiday_skip_sql`], so the date the page shows is
//! the date that gets minted.
//!
//! The api's `bank_holidays_sync` loop fills `bank_holidays` from GOV.UK;
//! only the api writes it, so the writer lives there.

/// England and Wales: GOV.UK's `england-and-wales` division, and the one a
/// user who hasn't chosen gets ([`DEFAULT_DIVISION`]).
pub const ENGLAND_AND_WALES: &str = "england-and-wales";
/// GOV.UK's `scotland` division.
pub const SCOTLAND: &str = "scotland";
/// GOV.UK's `northern-ireland` division.
pub const NORTHERN_IRELAND: &str = "northern-ireland";

/// Every division `bank_holidays.division` and
/// `users.bank_holiday_division` accept (their CHECKs).
pub const DIVISIONS: [&str; 3] = [ENGLAND_AND_WALES, SCOTLAND, NORTHERN_IRELAND];

/// The division read for a user with no `bank_holiday_division` (§8.3).
pub const DEFAULT_DIVISION: &str = ENGLAND_AND_WALES;

/// Whether `division` is one of [`DIVISIONS`].
#[must_use]
pub fn is_division(division: &str) -> bool {
    DIVISIONS.contains(&division)
}

/// SQL for a template owner's division: their `bank_holiday_division`, or
/// [`DEFAULT_DIVISION`] when they haven't chosen one. `template` is the
/// outer query's alias for a `journey_templates` row.
///
/// Reads `users` (`id`, `bank_holiday_division`; db-grants.yaml).
#[must_use]
pub fn owner_division_sql(template: &str) -> String {
    format!(
        "COALESCE((SELECT bh_u.bank_holiday_division FROM users bh_u \
                    WHERE bh_u.id = {template}.user_id), '{DEFAULT_DIVISION}')"
    )
}

/// SQL boolean: whether the template skips the service date `date` (an SQL
/// `DATE` expression) as a bank holiday. True when the template's
/// `skip_bank_holidays` is set and `bank_holidays` lists `date` in the
/// owner's division ([`owner_division_sql`]).
///
/// An empty `bank_holidays` (the sync never ran, or it is off) skips
/// nothing: every date mints as it did before bank holidays existed.
///
/// Reads `journey_templates` (`user_id`, `skip_bank_holidays`), `users`
/// and `bank_holidays` (db-grants.yaml).
#[must_use]
pub fn bank_holiday_skip_sql(template: &str, date: &str) -> String {
    let division = owner_division_sql(template);
    format!(
        "({template}.skip_bank_holidays AND EXISTS ( \
             SELECT 1 FROM bank_holidays bh_h \
              WHERE bh_h.division = {division} \
                AND bh_h.holiday_date = ({date})))"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_divisions_are_govuks_keys() {
        assert_eq!(
            DIVISIONS,
            ["england-and-wales", "scotland", "northern-ireland"]
        );
        assert!(is_division(SCOTLAND));
        assert!(!is_division("wales"));
        assert!(is_division(DEFAULT_DIVISION));
    }

    #[test]
    fn the_sql_names_the_outer_alias_only_through_its_columns() {
        let sql = bank_holiday_skip_sql("jx", "$1::date");
        assert!(sql.contains("jx.skip_bank_holidays"));
        assert!(sql.contains("jx.user_id"));
        assert!(sql.contains("($1::date)"));
        assert!(!sql.contains(" jt."), "no stray alias: {sql}");
    }
}

#[cfg(test)]
mod db_tests {
    use chrono::NaiveDate;
    use sqlx::PgPool;

    use super::*;

    const USER: &str = "bank-holiday-test-user";

    fn day(m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2050, m, d).unwrap()
    }

    async fn template(pool: &PgPool, skip: bool) -> i64 {
        crate::test_support::seed_user(pool, USER).await;
        sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, days_of_week, skip_bank_holidays) \
             VALUES ($1, 127, $2) RETURNING id",
        )
        .bind(USER)
        .bind(skip)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn holiday(pool: &PgPool, division: &str, date: NaiveDate) {
        sqlx::query(
            "INSERT INTO bank_holidays (division, holiday_date, title, fetched_at) \
             VALUES ($1, $2, 'A holiday', now())",
        )
        .bind(division)
        .bind(date)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn division(pool: &PgPool, division: Option<&str>) {
        sqlx::query("UPDATE users SET bank_holiday_division = $2 WHERE id = $1")
            .bind(USER)
            .bind(division)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn skips(pool: &PgPool, template_id: i64, date: NaiveDate) -> bool {
        let sql = format!(
            "SELECT {} FROM journey_templates jt WHERE jt.id = $1",
            bank_holiday_skip_sql("jt", "$2::date")
        );
        sqlx::query_scalar(&sql)
            .bind(template_id)
            .bind(date)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn an_empty_table_skips_nothing(pool: PgPool) {
        let t = template(&pool, true).await;
        assert!(!skips(&pool, t, day(4, 11)).await);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_owners_division_decides(pool: PgPool) {
        // Easter Monday 2050 is a bank holiday in England and Wales and
        // Northern Ireland, not in Scotland; 2 January only in Scotland.
        let easter_monday = day(4, 11);
        let second_january = day(1, 3);
        let t = template(&pool, true).await;
        holiday(&pool, ENGLAND_AND_WALES, easter_monday).await;
        holiday(&pool, NORTHERN_IRELAND, easter_monday).await;
        holiday(&pool, SCOTLAND, second_january).await;

        // No division chosen: England and Wales.
        assert!(skips(&pool, t, easter_monday).await);
        assert!(!skips(&pool, t, second_january).await);
        assert!(!skips(&pool, t, day(4, 12)).await);

        division(&pool, Some(SCOTLAND)).await;
        assert!(!skips(&pool, t, easter_monday).await);
        assert!(skips(&pool, t, second_january).await);

        division(&pool, Some(NORTHERN_IRELAND)).await;
        assert!(skips(&pool, t, easter_monday).await);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_switch_off_skips_nothing(pool: PgPool) {
        let t = template(&pool, false).await;
        holiday(&pool, ENGLAND_AND_WALES, day(4, 11)).await;
        assert!(!skips(&pool, t, day(4, 11)).await);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn defaults_and_checks(pool: PgPool) {
        crate::test_support::seed_user(&pool, USER).await;
        let skip: bool = sqlx::query_scalar(
            "INSERT INTO journey_templates (user_id, days_of_week) VALUES ($1, 31) \
             RETURNING skip_bank_holidays",
        )
        .bind(USER)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(skip, "decision 8: on by default");
        let chosen: Option<String> =
            sqlx::query_scalar("SELECT bank_holiday_division FROM users WHERE id = $1")
                .bind(USER)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(chosen, None);

        for sql in [
            "UPDATE users SET bank_holiday_division = 'wales' WHERE id = $1",
            "INSERT INTO bank_holidays (division, holiday_date, title, fetched_at) \
             SELECT 'wales', '2050-01-01', $1, now()",
            "INSERT INTO bank_holidays_sync (id, last_attempt_at, last_result) \
             SELECT FALSE, now(), 'ok' WHERE $1 IS NOT NULL",
            "INSERT INTO bank_holidays_sync (last_attempt_at, last_result) \
             SELECT now(), 'maybe' WHERE $1 IS NOT NULL",
        ] {
            let err = sqlx::query(sql)
                .bind(USER)
                .execute(&pool)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("check constraint"), "{sql}: {err}");
        }
        for division in DIVISIONS {
            sqlx::query("UPDATE users SET bank_holiday_division = $2 WHERE id = $1")
                .bind(USER)
                .bind(division)
                .execute(&pool)
                .await
                .unwrap();
        }
    }
}
