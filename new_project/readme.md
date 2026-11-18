# 🏦 Banking Data Platform — BigQuery SQL Reference

**Project:** `dbs-data-ai-ai-core`
**Region:** `us-central1`
**Datasets:** `sreeram_raw` (landing) → `sreeram_target` (curated) → `sreeram_kpi` (reporting)
**Prepared:** 10 October 2026

---

## 📋 Table of Contents

1. [Architecture at a Glance](#1-architecture-at-a-glance)
2. [Datasets Summary](#2-datasets-summary)
3. [Daily Incremental Merge SQL](#3-daily-incremental-merge-sql)
   - 3.1 [Customers — SCD Type 2](#31-customers--scd-type-2-merge)
   - 3.2 [Accounts — Type 1 Upsert](#32-accounts--type-1-upsert-merge)
   - 3.3 [Transactions — Insert-Only](#33-transactions--insert-only-merge)
4. [KPI Reporting Views (`sreeram_kpi`)](#4-kpi-reporting-views-sreeram_kpi)
   - 4.0 [Dataset Creation](#40-dataset-creation)
   - 4.1 [KPI 1 — Top 5 Customers by Volume](#41-kpi-1--top-5-customers-by-transaction-volume)
   - 4.2 [KPI 2 — Product Contribution](#42-kpi-2--product-wise-value-contribution)
   - 4.3 [KPI 3 — Inactive Accounts](#43-kpi-3--inactive-accounts)
   - 4.4 [KPI 4 — Suspicious Transactions](#44-kpi-4--suspicious-high-value-transactions)
   - 4.5 [KPI 5 — Customer Profile History](#45-kpi-5--customer-profile-history-scd2-shape)
   - 4.6 [KPI 6 — Loan Repayment Health](#46-kpi-6--loan-portfolio--repayment-health)
   - 4.7 [KPI 7 — Card Adoption & Cross-Sell](#47-kpi-7--card-adoption--cross-sell-candidates)
5. [Verification Queries](#5-verification-queries)
6. [Assumptions & Things to Know](#6-assumptions--things-to-know)

---

## 1. Architecture at a Glance

```
   📁 GCS                🚀 Dataproc PySpark             🗄️  BigQuery
┌──────────────┐      ┌──────────────────────┐      ┌─────────────────────┐
│ file_0.rtf   │─────▶│ ingest_banking_raw_   │─────▶│  sreeram_target      │
│ (historical) │      │ pyspark.py            │      │  (curated, current)  │
└──────────────┘      └──────────────────────┘      └─────────┬───────────┘
                                                                │
┌──────────────┐      ┌──────────────────────┐                │
│ file_1.rtf   │─────▶│ daily_preprocess_     │─────▶ sreeram_raw            │
│ (daily feed) │      │ sreeram.py             │      (landing + DQ log)     │
└──────────────┘      └──────────┬───────────┘                │
                                  │          MERGE (SCD2 / upsert / insert)  │
                                  └───────────────────────────▶┘
                                                                │
                                                     ┌──────────▼───────────┐
                                                     │  sreeram_kpi          │
                                                     │  (7 reporting views)  │
                                                     └───────────────────────┘
```

| Stage | What happens | Dataset |
|---|---|---|
| 🧾 Historical load | One-time load of `file_0.rtf`, 10 entities, partitioned + clustered tables | `sreeram_target` |
| 📥 Daily landing | Daily file lands **as-is**, no rules applied yet | `sreeram_raw.*_raw` |
| 🔍 Data-quality rules | Duplicate identity, missing fields, sort-code mismatch, overdraft breach, structuring, dormant-reactivation, etc. | `sreeram_raw.dq_results` |
| 🧹 Cleansed stage | Rows that passed validation, typed and ready to merge | `sreeram_raw.*_stage` |
| 🔀 Merge into target | SCD2 (customers), upsert (accounts), insert-only (transactions) | `sreeram_target` |
| 📊 Reporting | Seven KPI views answering the business questions | `sreeram_kpi` |

---

## 2. Datasets Summary

| Icon | Dataset | Purpose | Key tables |
|---|---|---|---|
| 🗄️ | `sreeram_target` | Single current version of the business data | `customers`, `accounts`, `transactions`, `branches`, `cards`, `loans`, `loan_payments`, `account_types`, `transaction_types` |
| 📦 | `sreeram_raw` | Daily landing zone + data-quality audit trail | `customers_raw`, `accounts_raw`, `customers_stage`, `accounts_stage`, `dq_results` |
| 📈 | `sreeram_kpi` | Read-only views for dashboards and reporting | `kpi1_top5_customers_by_volume` … `kpi7_card_cross_sell` |

---

## 3. Daily Incremental Merge SQL

These three statements run automatically at the end of `daily_preprocess_sreeram.py`, once the day's file has been landed, validated, and staged. All three read from `sreeram_raw.*_stage` and write to `sreeram_target`.

### 3.1 Customers — SCD Type 2 Merge

**🧬 What SCD Type 2 means here:** every customer has a row with `valid_from`, `valid_to`, and `is_current`. When a tracked attribute changes (name, email, phone, address, segment, KYC status), the old row is closed (`is_current = FALSE`, `valid_to` set) and a new row is inserted (`is_current = TRUE`, `valid_to = 9999-12-31`). Email and phone are compared after normalising case/format, so a formatting-only change (e.g. `+44 909 879 0997` vs `+44(0)909 879 0997`) does **not** create a new version.

**Step 1 — one-time setup** (adds the history columns and backfills the first version for every existing customer):

```sql
ALTER TABLE `dbs-data-ai-ai-core.sreeram_target.customers`
  ADD COLUMN IF NOT EXISTS valid_from DATE,
  ADD COLUMN IF NOT EXISTS valid_to DATE,
  ADD COLUMN IF NOT EXISTS is_current BOOL;

UPDATE `dbs-data-ai-ai-core.sreeram_target.customers`
SET valid_from = as_of_date,
    valid_to = DATE '9999-12-31',
    is_current = TRUE
WHERE is_current IS NULL;
```

**Step 2 — runs after every daily batch:**

```sql
MERGE `dbs-data-ai-ai-core.sreeram_target.customers` AS tgt
USING (
  WITH cur AS (
    SELECT
      c.*,
      LOWER(TRIM(c.Email)) AS email_n,
      CASE
        WHEN REGEXP_REPLACE(REGEXP_REPLACE(c.Phone, r'\(0\)', ''), r'[^0-9]', '') LIKE '0%'
          THEN CONCAT('44', SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(c.Phone, r'\(0\)', ''), r'[^0-9]', ''), 2))
        ELSE REGEXP_REPLACE(REGEXP_REPLACE(c.Phone, r'\(0\)', ''), r'[^0-9]', '')
      END AS phone_n
    FROM `dbs-data-ai-ai-core.sreeram_target.customers` AS c
    WHERE c.is_current
  ),
  changed AS (
    SELECT s.*, cur.CustomerID AS current_id
    FROM `dbs-data-ai-ai-core.sreeram_raw.customers_stage` AS s
    LEFT JOIN cur ON cur.CustomerID = s.CustomerID
    WHERE cur.CustomerID IS NULL
       OR TO_JSON_STRING(STRUCT(s.FirstName, s.LastName, s.Email, s.Phone, s.AddressLine1,
                                s.Town, s.County, s.PostCode, s.CustomerSegment, s.KYCStatus))
       <> TO_JSON_STRING(STRUCT(cur.FirstName, cur.LastName, cur.email_n AS Email, cur.phone_n AS Phone,
                                cur.AddressLine1, cur.Town, cur.County, cur.PostCode,
                                cur.CustomerSegment, cur.KYCStatus))
  )
  -- row 1: matches the current version, which gets closed
  SELECT CustomerID AS merge_key, * EXCEPT (current_id) FROM changed
  UNION ALL
  -- row 2: NULL key never matches, so the new version is inserted
  SELECT NULL AS merge_key, * EXCEPT (current_id) FROM changed WHERE current_id IS NOT NULL
) AS src
ON tgt.CustomerID = src.merge_key AND tgt.is_current
WHEN MATCHED THEN UPDATE SET
  is_current = FALSE,
  valid_to = DATE_SUB(src.as_of_date, INTERVAL 1 DAY)
WHEN NOT MATCHED THEN INSERT (
  CustomerID, FirstName, LastName, DateOfBirth, NationalInsuranceNumber, Email, Phone,
  AddressLine1, Town, County, PostCode, CustomerSegment, KYCStatus, CreatedDate,
  batch_id, as_of_date, valid_from, valid_to, is_current)
VALUES (
  src.CustomerID, src.FirstName, src.LastName, src.DateOfBirth, src.NationalInsuranceNumber,
  src.Email, src.Phone, src.AddressLine1, src.Town, src.County, src.PostCode,
  src.CustomerSegment, src.KYCStatus, src.CreatedDate,
  src.batch_id, src.as_of_date, src.as_of_date, DATE '9999-12-31', TRUE);
```

> ⚠️ **Limitation:** the one-time historical loader (`ingest_banking_raw_pyspark.py`) overwrites `customers` on every run. Running it again after SCD2 history has started will erase that history.

---

### 3.2 Accounts — Type 1 Upsert Merge

Accounts are **not** versioned (Type 1): the latest values simply overwrite the previous row. This keeps balance, status, and branch changes current without growing a history table.

```sql
MERGE `dbs-data-ai-ai-core.sreeram_target.accounts` AS tgt
USING `dbs-data-ai-ai-core.sreeram_raw.accounts_stage` AS src
ON tgt.AccountID = src.AccountID
WHEN MATCHED THEN UPDATE SET
  tgt.CustomerID      = src.CustomerID,
  tgt.BranchID        = src.BranchID,
  tgt.AccountTypeID   = src.AccountTypeID,
  tgt.SortCode        = src.SortCode,
  tgt.AccountNumber   = src.AccountNumber,
  tgt.OpenDate        = src.OpenDate,
  tgt.CloseDate       = src.CloseDate,
  tgt.Status          = src.Status,
  tgt.CurrentBalance  = src.CurrentBalance,
  tgt.Currency        = src.Currency,
  tgt.batch_id        = src.batch_id,
  tgt.as_of_date      = src.as_of_date
WHEN NOT MATCHED THEN INSERT (
  AccountID, CustomerID, BranchID, AccountTypeID, SortCode, AccountNumber,
  OpenDate, CloseDate, Status, CurrentBalance, Currency, batch_id, as_of_date)
VALUES (
  src.AccountID, src.CustomerID, src.BranchID, src.AccountTypeID, src.SortCode, src.AccountNumber,
  src.OpenDate, src.CloseDate, src.Status, src.CurrentBalance, src.Currency, src.batch_id, src.as_of_date);
```

---

### 3.3 Transactions — Insert-Only Merge

Transactions are immutable facts, so the merge only inserts `TransactionID`s that are not already in the target. Exact duplicates and transactions already loaded are filtered out earlier, by the data-quality rules (`EXACT_DUPLICATE`, `ALREADY_LOADED`), so this statement only ever adds genuinely new rows.

```sql
MERGE `dbs-data-ai-ai-core.sreeram_target.transactions` AS tgt
USING `dbs-data-ai-ai-core.sreeram_raw.transactions_stage` AS src
ON tgt.TransactionID = src.TransactionID
WHEN NOT MATCHED THEN INSERT (
  TransactionID, AccountID, TransactionTypeID, TransactionDateTime, Amount, Currency,
  Channel, MerchantCategory, Description, BalanceAfter, EmployeeID, batch_id, as_of_date)
VALUES (
  src.TransactionID, src.AccountID, src.TransactionTypeID, src.TransactionDateTime, src.Amount, src.Currency,
  src.Channel, src.MerchantCategory, src.Description, src.BalanceAfter, src.EmployeeID, src.batch_id, src.as_of_date);
```

---

## 4. KPI Reporting Views (`sreeram_kpi`)

Each view below answers one business question from the "Required Analytical KPIs" list. All seven read live from `sreeram_target`, so they reflect the most recent merge automatically — no separate refresh step is needed.

### 4.0 Dataset Creation

```sql
CREATE SCHEMA IF NOT EXISTS `dbs-data-ai-ai-core.sreeram_kpi`
OPTIONS (location = 'us-central1');
```

---

### 4.1 KPI 1 — Top 5 Customers by Transaction Volume

| | |
|---|---|
| 💰 **Business question** | Who moved the most money through their accounts? |
| 📐 **Rule** | Sum of absolute transaction amounts per customer, ranked, top 5 only |
| ✅ **Verified result** | Daniel Swift, Teresa Webb, Gavin Holmes, Oliver Adams, Henry Hussain |

```sql
CREATE OR REPLACE VIEW `dbs-data-ai-ai-core.sreeram_kpi.kpi1_top5_customers_by_volume` AS
WITH customer_volume AS (
  SELECT
    a.CustomerID,
    COUNT(*)                AS transaction_count,
    SUM(ABS(t.Amount))      AS transaction_volume
  FROM `dbs-data-ai-ai-core.sreeram_target.transactions` AS t
  JOIN `dbs-data-ai-ai-core.sreeram_target.accounts`     AS a
    ON t.AccountID = a.AccountID
  GROUP BY a.CustomerID
)
SELECT
  RANK() OVER (ORDER BY v.transaction_volume DESC) AS volume_rank,
  c.CustomerID,
  c.FirstName,
  c.LastName,
  v.transaction_count,
  v.transaction_volume
FROM customer_volume AS v
JOIN `dbs-data-ai-ai-core.sreeram_target.customers` AS c
  ON v.CustomerID = c.CustomerID
ORDER BY v.transaction_volume DESC
LIMIT 5;
```

---

### 4.2 KPI 2 — Product-Wise Value Contribution

| | |
|---|---|
| 📊 **Business question** | Which account types contribute most transaction value? |
| 📐 **Rule** | Transaction value and share-of-total, grouped by account type |
| ✅ **Verified result** | BusinessChecking 22.9% · CD-12Month 21.6% · Savings 21.3% · Checking 18.3% · MoneyMarket 15.9% |

```sql
CREATE OR REPLACE VIEW `dbs-data-ai-ai-core.sreeram_kpi.kpi2_product_contribution` AS
WITH type_value AS (
  SELECT
    at.AccountTypeID,
    at.TypeName,
    COUNT(*)           AS transaction_count,
    SUM(ABS(t.Amount)) AS transaction_value
  FROM `dbs-data-ai-ai-core.sreeram_target.transactions`    AS t
  JOIN `dbs-data-ai-ai-core.sreeram_target.accounts`        AS a
    ON t.AccountID = a.AccountID
  JOIN `dbs-data-ai-ai-core.sreeram_target.account_types`   AS at
    ON a.AccountTypeID = at.AccountTypeID
  GROUP BY at.AccountTypeID, at.TypeName
)
SELECT
  AccountTypeID,
  TypeName,
  transaction_count,
  transaction_value,
  SAFE_DIVIDE(transaction_value, SUM(transaction_value) OVER ()) AS value_share,
  RANK() OVER (ORDER BY transaction_value DESC)                  AS value_rank
FROM type_value
ORDER BY value_rank;
```

---

### 4.3 KPI 3 — Inactive Accounts

| | |
|---|---|
| 💤 **Business question** | Which accounts have had no transaction activity in the last 3 months? |
| 📐 **Rule** | No transaction since `(latest transaction date) − 3 months`, or never transacted. Closed accounts excluded |
| ✅ **Verified result** | 442 accounts, of which 25 have never transacted |

```sql
CREATE OR REPLACE VIEW `dbs-data-ai-ai-core.sreeram_kpi.kpi3_inactive_accounts` AS
WITH ref AS (
  SELECT DATE(MAX(TransactionDateTime)) AS ref_date
  FROM `dbs-data-ai-ai-core.sreeram_target.transactions`
),
last_tx AS (
  SELECT
    AccountID,
    DATE(MAX(TransactionDateTime)) AS last_transaction_date
  FROM `dbs-data-ai-ai-core.sreeram_target.transactions`
  GROUP BY AccountID
)
SELECT
  a.AccountID,
  a.CustomerID,
  a.BranchID,
  a.AccountTypeID,
  a.Status,
  l.last_transaction_date,
  DATE_DIFF(ref.ref_date, l.last_transaction_date, DAY) AS days_since_last_tx,
  ref.ref_date                                          AS reference_date
FROM `dbs-data-ai-ai-core.sreeram_target.accounts` AS a
CROSS JOIN ref
LEFT JOIN last_tx AS l
  ON a.AccountID = l.AccountID
WHERE a.Status <> 'Closed'
  AND (
    l.last_transaction_date IS NULL
    OR l.last_transaction_date < DATE_SUB(ref.ref_date, INTERVAL 3 MONTH)
  );
```

> ℹ️ The reference date is the **latest transaction date in the data** (2026-06-11), not `CURRENT_DATE()`. Swap in `CURRENT_DATE()` if the warehouse is kept fully up to date going forward.

---

### 4.4 KPI 4 — Suspicious (High-Value) Transactions

| | |
|---|---|
| 🚨 **Business question** | Which transactions exceed the high-value threshold (e.g. £100,000)? |
| 📐 **Rule** | `ABS(Amount) > 100000`, GBP only (no FX table is available for other currencies) |
| ✅ **Verified result** | 6 transactions |

```sql
CREATE OR REPLACE VIEW `dbs-data-ai-ai-core.sreeram_kpi.kpi4_suspicious_transactions` AS
SELECT
  t.TransactionID,
  t.AccountID,
  a.CustomerID,
  t.TransactionDateTime,
  t.Amount,
  t.Currency,
  tt.TypeName        AS transaction_type,
  t.Channel,
  t.Description,
  t.BalanceAfter,
  t.EmployeeID
FROM `dbs-data-ai-ai-core.sreeram_target.transactions`       AS t
JOIN `dbs-data-ai-ai-core.sreeram_target.accounts`           AS a
  ON t.AccountID = a.AccountID
JOIN `dbs-data-ai-ai-core.sreeram_target.transaction_types`  AS tt
  ON t.TransactionTypeID = tt.TransactionTypeID
WHERE t.Currency = 'GBP'
  AND ABS(t.Amount) > 100000;
```

---

### 4.5 KPI 5 — Customer Profile History (SCD2 Shape)

| | |
|---|---|
| 🧬 **Business question** | How have customer attributes changed across Day 0, Day 1, and Day 2? |
| 📐 **Rule** | One row per customer with SCD2 columns (`valid_from`, `valid_to`, `is_current`) |
| ⚠️ **Limitation** | True multi-day history exists only once the daily SCD2 merge (section 3.1) has run more than once. |

```sql
CREATE OR REPLACE VIEW `dbs-data-ai-ai-core.sreeram_kpi.kpi5_customer_profile_history` AS
SELECT
  c.CustomerID,
  c.FirstName,
  c.LastName,
  c.CustomerSegment,
  c.KYCStatus,
  c.AddressLine1,
  c.Town,
  c.County,
  c.PostCode,
  (
    SELECT COUNT(*)
    FROM `dbs-data-ai-ai-core.sreeram_target.accounts` AS a
    WHERE a.CustomerID = c.CustomerID
      AND a.Status = 'Active'
  )                    AS active_accounts,
  c.batch_id,
  c.as_of_date         AS valid_from,
  DATE '9999-12-31'    AS valid_to,
  TRUE                 AS is_current
FROM `dbs-data-ai-ai-core.sreeram_target.customers` AS c;
```

> 🔒 **Privacy note:** `NationalInsuranceNumber` is deliberately excluded from this view.

---

### 4.6 KPI 6 — Loan Portfolio & Repayment Health

| | |
|---|---|
| 🏠 **Business question** | Which branches/customers show loan or repayment risk? |
| 📐 **Rule** | `at_risk = TRUE` when a loan is Delinquent/Default, **or** Active with at least one Missed payment |
| ✅ **Verified result** | 42 loans at risk out of 110 |

```sql
CREATE OR REPLACE VIEW `dbs-data-ai-ai-core.sreeram_kpi.kpi6_loan_repayment_health` AS
WITH pay AS (
  SELECT
    LoanID,
    COUNT(*)                          AS payment_count,
    COUNTIF(PaymentStatus = 'OnTime') AS on_time_count,
    COUNTIF(PaymentStatus = 'Late')   AS late_count,
    COUNTIF(PaymentStatus = 'Missed') AS missed_count,
    COUNTIF(PaymentStatus = 'Partial') AS partial_count
  FROM `dbs-data-ai-ai-core.sreeram_target.loan_payments`
  GROUP BY LoanID
)
SELECT
  l.LoanID,
  l.CustomerID,
  l.BranchID,
  b.BranchName,
  l.LoanType,
  l.Status                                  AS loan_status,
  l.PrincipalAmount,
  l.InterestRate,
  l.StartDate,
  COALESCE(p.payment_count, 0)              AS payment_count,
  COALESCE(p.on_time_count, 0)              AS on_time_count,
  COALESCE(p.late_count, 0)                 AS late_count,
  COALESCE(p.missed_count, 0)               AS missed_count,
  COALESCE(p.partial_count, 0)              AS partial_count,
  SAFE_DIVIDE(
    COALESCE(p.late_count, 0) + COALESCE(p.missed_count, 0),
    COALESCE(p.payment_count, 0)
  )                                         AS late_or_missed_rate,
  (
    l.Status IN ('Delinquent', 'Default')
    OR (l.Status = 'Active' AND COALESCE(p.missed_count, 0) > 0)
  )                                         AS at_risk
FROM `dbs-data-ai-ai-core.sreeram_target.loans` AS l
LEFT JOIN `dbs-data-ai-ai-core.sreeram_target.branches` AS b
  ON l.BranchID = b.BranchID
LEFT JOIN pay AS p
  ON l.LoanID = p.LoanID;
```

---

### 4.7 KPI 7 — Card Adoption & Cross-Sell Candidates

| | |
|---|---|
| 💳 **Business question** | Which customers have active cards, and which are candidates for cross-sell? |
| 📐 **Rule** | `has_active_card` = at least one card Active and not expired at the reference date. `cross_sell_candidate` = an Active account but no active card |
| ✅ **Verified result** | 149 customers with an active card · 105 cross-sell candidates |

```sql
CREATE OR REPLACE VIEW `dbs-data-ai-ai-core.sreeram_kpi.kpi7_card_cross_sell` AS
WITH ref AS (
  SELECT DATE(MAX(TransactionDateTime)) AS ref_date
  FROM `dbs-data-ai-ai-core.sreeram_target.transactions`
),
card_summary AS (
  SELECT
    a.CustomerID,
    COUNTIF(c.CardType = 'Credit') AS active_credit_cards,
    COUNTIF(c.CardType = 'Debit')  AS active_debit_cards
  FROM `dbs-data-ai-ai-core.sreeram_target.cards`    AS c
  JOIN `dbs-data-ai-ai-core.sreeram_target.accounts` AS a
    ON c.AccountID = a.AccountID
  CROSS JOIN ref
  WHERE c.Status = 'Active'
    AND DATE(c.ExpiryDate) >= ref.ref_date
  GROUP BY a.CustomerID
),
account_summary AS (
  SELECT
    CustomerID,
    COUNTIF(Status = 'Active') AS active_accounts
  FROM `dbs-data-ai-ai-core.sreeram_target.accounts`
  GROUP BY CustomerID
)
SELECT
  c.CustomerID,
  c.FirstName,
  c.LastName,
  c.CustomerSegment,
  COALESCE(acc.active_accounts, 0)        AS active_accounts,
  COALESCE(cs.active_credit_cards, 0)     AS active_credit_cards,
  COALESCE(cs.active_debit_cards, 0)      AS active_debit_cards,
  (COALESCE(cs.active_credit_cards, 0) + COALESCE(cs.active_debit_cards, 0)) > 0
                                          AS has_active_card,
  (
    COALESCE(acc.active_accounts, 0) > 0
    AND (COALESCE(cs.active_credit_cards, 0) + COALESCE(cs.active_debit_cards, 0)) = 0
  )                                       AS cross_sell_candidate
FROM `dbs-data-ai-ai-core.sreeram_target.customers` AS c
LEFT JOIN card_summary AS cs
  ON c.CustomerID = cs.CustomerID
LEFT JOIN account_summary AS acc
  ON c.CustomerID = acc.CustomerID;
```

---

## 5. Verification Queries

Run these after any load to confirm the pipeline behaved as expected.

**✅ Historical load audit (per run):**
```sql
SELECT source_table, target_table, source_rows, footer_rows, loaded_rows, status
FROM `dbs-data-ai-ai-core.sreeram_target.load_audit`
WHERE run_id = '<paste run_id from the job log>'
ORDER BY source_table;
```

**✅ Customer SCD2 row counts:**
```sql
SELECT COUNT(*) AS total_rows, COUNTIF(is_current) AS current_rows
FROM `dbs-data-ai-ai-core.sreeram_target.customers`;
```

**✅ Account row count:**
```sql
SELECT COUNT(*) AS account_rows
FROM `dbs-data-ai-ai-core.sreeram_target.accounts`;
```

**✅ Data-quality outcomes for one run:**
```sql
SELECT check_name, action, COUNT(*) AS n
FROM `dbs-data-ai-ai-core.sreeram_raw.dq_results`
WHERE run_id = '<paste run_id from the job log>'
GROUP BY check_name, action
ORDER BY check_name;
```

---

## 6. Assumptions & Things to Know

| Icon | Note |
|---|---|
| 📅 | Time-based KPIs (3, 7) anchor to the **latest transaction date in the data**, not `CURRENT_DATE()`, because the source data ends mid-2026. |
| 💷 | KPI 4's £100,000 threshold applies to **GBP transactions only** — there is no FX conversion table for the small number of AED/USD/EUR/CHF rows. |
| 🧬 | KPI 5 is SCD2-*shaped* but needs at least two daily merges to show real history. |
| 🔒 | `NationalInsuranceNumber` is withheld from reporting views. `sreeram_target.customers` itself still holds it and should have restricted access. |
| ♻️ | All three daily merges (3.1–3.3) are **idempotent** — re-running the same batch does not duplicate data, because the raw/stage tables are cleared per `batch_id` before being rewritten. |
| 🚫 | Known pre-existing data issue: 18 National Insurance numbers in the historical load are shared by two different `CustomerID`s. The daily pipeline blocks *new* conflicts but does not retroactively fix these. |

---

*Generated for the Dataproc + BigQuery incremental banking pipeline — hackathon-banglore / cluster-4922, project `dbs-data-ai-ai-core`.*
