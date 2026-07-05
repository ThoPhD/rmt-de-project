# Analytical Report Design Document: RMT Transaction Analytics

This document details the analytical report designs, business scenarios, key performance indicators (KPIs), and practical SQL queries built on top of the ClickHouse dbt dimensional model for the Remittance & Transaction Management (RMT) system.

---

## 1. Business Analysis Scenarios

To drive strategic growth and maintain strict regulatory compliance, the RMT data platform supports three main business analysis scenarios:

### A. Daily Transactional Volume Trends
* **Business Use Case**: Tracks the daily velocity, count, and dollar volume of transactions passing through the platform. 
* **Importance**:
  * **System Health & Monitoring**: A sudden drop in transaction count or volume serves as a high-priority operational alert (e.g., API gateway failures, downstream liquidity provider downtime).
  * **Growth & Market Adoption**: Provides senior leadership with a high-level view of platform scaling, seasonality (e.g., weekend vs. weekday traffic), and overall adoption.
  * **Liquidity Management**: Financial operations teams monitor these trends to ensure sufficient fiat/crypto reserves are maintained in the hot wallets to satisfy customer demand.

### B. Customer Demographic & KYC Level Analysis
* **Business Use Case**: Analyzes user behavior, transacting power, and volume distributions aggregated by Know Your Customer (KYC) levels.
* **Importance**:
  * **Product Tiering & Limits**: High-KYC levels (e.g., Level 2 or 3) are typically granted higher daily/monthly transacting limits. Analysts inspect this data to see if users are hitting their limits and if there is a business case to prompt them to upgrade.
  * **Customer Profiling**: Helps marketing and product teams identify which customer cohorts (segmented by compliance level) drive the highest volume, enabling targeted retention campaigns.
  * **UX Optimization**: Correlating KYC levels with transaction failure or completion rates helps pinpoint onboarding bottlenecks.

### C. Financial Transaction Audit Trail
* **Business Use Case**: Provides a chronological, immutable ledger mapping transactions to user profiles, capturing both their historical state (at transaction time) and current state.
* **Importance**:
  * **Regulatory & AML/CFT Compliance**: Regulatory bodies require tracking the exact compliance state of a user when a financial event occurred. For example, if a user performed a $50,000 transaction, they must have been at KYC Level 3 *at that moment*, even if they were subsequently downgraded to Level 0 due to suspicious activity.
  * **SCD Type II Reconciliation**: By leveraging the Slowly Changing Dimension (SCD) Type II logic captured in `users_snapshot`, this scenario allows auditors to trace the user's compliance lifecycle and detect compliance lapses or historical inconsistencies.

---

## 2. Key Performance Indicators (KPIs) & Metrics

Here is the formal calculation and business logic for the primary metrics utilized in our reporting layer:

### A. Transaction Counts
* **Definition**: The total number of unique transaction records in a given period.
* **ClickHouse Formula**: `COUNT(tx_id)`
* **Usage**:
  * **Total Transactions**: Calculated across all statuses (`completed`, `pending`, `failed`) to monitor overall network traffic.
  * **Completed Transactions**: Calculated specifically where `status = 'completed'` to measure successful throughput.

### B. Total Volumes in USD
* **Definition**: The equivalent value in US Dollars (USD) of all transactions, calculated by converting non-USD currencies using hourly exchange rates.
* **Calculation Logic**:
  * **Asset Selection**: In `int_transactions_enriched.sql`, the non-USDT currency of the pair is defined as the `asset_currency`, and its corresponding quantity as `asset_amount` (i.e. if a user buys BTC with USDT, or sells BTC for USDT, the asset is BTC).
  * **Rate Joining**: The `asset_currency` is joined against the `stg_rates` table on the currency pair symbol (e.g., `BTCUSDT`) and matched to the exact hour of the transaction (`dateTrunc('hour', tx.created_at) = r.open_time`).
  * **USD Conversion**: 
    $$\text{amount\_usd} = \text{asset\_amount} \times \text{exchange\_rate}$$
    * If the currency is already `USDT`, the exchange rate is defaulted to `1`.
    * If no hourly exchange rate is found in the rates table, it defaults to `0` (coalesced) to avoid NULL propagation.
  * **Metric Aggregation**: `SUM(amount_usd)`

### C. Active Users
* **Definition**: The number of unique users who initiated at least one transaction within a specific time window (e.g., Daily Active Users - DAU, Monthly Active Users - MAU).
* **ClickHouse Formula**: `COUNT(DISTINCT user_id)`
* **Usage**: Measures engagement density. An increase in volume alongside flat active user metrics indicates high-value whale activity, whereas an increase in active users indicates broad market adoption.

### D. KYC-Specific Volumes
* **Definition**: The total transaction volume in USD grouped by the user's KYC tier.
* **Calculation Logic**:
  * Crucially, this metric aggregates volumes by `kyc_level_at_transaction` rather than the user's current KYC level. This ensures historical accuracy by reflecting the user's actual clearance at the moment of execution.
  * **ClickHouse Formula**: `SUM(amount_usd) GROUP BY kyc_level_at_transaction`

---

## 3. Practical Analytical SQL Queries (ClickHouse)

These queries are fully compatible with ClickHouse SQL and query the final reporting views and tables within the `analytics` database.

### Query 1: Daily Transaction Volume, Activity, & SMA Trends
* **Goal**: Analyze daily transaction volume, transaction counts, and active users, along with a 7-day Simple Moving Average (SMA) to smooth out weekend volatility.
* **SQL Query**:
```sql
SELECT
    date_key,
    total_transactions,
    total_volume_usd,
    active_users,
    -- 7-Day Simple Moving Average (SMA) of Transaction Volume
    AVG(total_volume_usd) OVER (
        ORDER BY date_key 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS volume_usd_7d_sma,
    -- 7-Day Simple Moving Average (SMA) of Active Users
    AVG(active_users) OVER (
        ORDER BY date_key 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS active_users_7d_sma,
    -- Average transaction size per day
    if(total_transactions > 0, total_volume_usd / total_transactions, 0) AS avg_transaction_value_usd
FROM
    analytics.report_daily_volume_usd
ORDER BY
    date_key DESC;
```

### Query 2: KYC Level Volume Contribution & Share Analysis
* **Goal**: Compute the total completed transaction volume and count for each KYC level, including their percentage contribution to the platform's total performance.
* **SQL Query**:
```sql
SELECT
    kyc_level_at_transaction,
    completed_transaction_count,
    total_completed_volume_usd,
    -- Percentage contribution to total completed transaction count
    completed_transaction_count * 100.0 / SUM(completed_transaction_count) OVER () AS pct_share_transaction_count,
    -- Percentage contribution to total completed volume in USD
    total_completed_volume_usd * 100.0 / SUM(total_completed_volume_usd) OVER () AS pct_share_volume_usd,
    -- Average transaction value per KYC level
    if(completed_transaction_count > 0, total_completed_volume_usd / completed_transaction_count, 0) AS avg_volume_per_tx_usd
FROM
    analytics.report_completed_volume_by_kyc
ORDER BY
    kyc_level_at_transaction ASC;
```

### Query 3: Transaction Audit & User KYC Progression Tracking
* **Goal**: Identify transactions executed at a historical KYC level that differs from the user's current KYC level. This is crucial for verifying KYC upgrades over time and highlighting compliance anomalies (e.g., downgrades).
* **SQL Query**:
```sql
SELECT
    tx_id,
    transaction_at,
    user_id,
    historical_kyc_level,
    current_kyc_level,
    amount_usd,
    status,
    -- Categorize KYC transition type
    CASE
        WHEN current_kyc_level > historical_kyc_level THEN 'Upgraded KYC (User progressed)'
        WHEN current_kyc_level < historical_kyc_level THEN 'Downgraded KYC (Compliance flag/Revocation)'
        ELSE 'No Change'
    END AS kyc_transition_status,
    -- Calculate difference in levels
    CAST(current_kyc_level AS Int8) - CAST(historical_kyc_level AS Int8) AS kyc_level_diff
FROM
    analytics.report_transaction_audit
WHERE
    historical_kyc_level != current_kyc_level
ORDER BY
    transaction_at DESC
LIMIT 100;
```

### Query 4: Monthly Popular Destination Currencies by KYC Level (Advanced Slice)
* **Goal**: Deep dive into the dimensional model (`fact_transactions` and `dim_date`) to analyze which destination currencies are favored by different KYC tiers month-over-month.
* **SQL Query**:
```sql
SELECT
    toStartOfMonth(f.date_key) AS transaction_month,
    f.kyc_level_at_transaction AS kyc_level,
    f.destination_currency AS currency,
    COUNT(f.tx_id) AS tx_count,
    SUM(f.amount_usd) AS volume_usd,
    RANK() OVER (
        PARTITION BY transaction_month, f.kyc_level_at_transaction 
        ORDER BY SUM(f.amount_usd) DESC
    ) AS currency_rank
FROM
    analytics.fact_transactions f
INNER JOIN
    analytics.dim_date d ON f.date_key = d.date_key
WHERE
    f.status = 'completed'
GROUP BY
    transaction_month,
    kyc_level,
    currency
ORDER BY
    transaction_month DESC,
    kyc_level ASC,
    currency_rank ASC;
```

---

## 4. Recommendations for Future Business Reports and Metrics

To expand the analytical capabilities of the RMT system, we recommend implementing the following metrics and data structures in subsequent releases:

1. **Velocity and Threshold Alert Reports (AML Compliance)**:
   * *Metric*: Velocity of transactions (e.g., total USD volume transacted by a single user in rolling 24-hour and 30-day windows).
   * *Objective*: Automatically flag users whose volume approaches or exceeds the regulatory thresholds of their current KYC level (e.g., flagging KYC Level 1 users transacting over $5,000 in 24 hours).

2. **User Retention & Cohort Analysis**:
   * *Metric*: Cohort Month (based on user creation date) linked to transactional frequency.
   * *Objective*: Track user lifetime value (LTV) and churn rates by measuring how long user groups remain active on the platform after registration.

3. **Failed Transaction & Liquidity Health Analysis**:
   * *Metric*: Transaction Failure Rate (Failed / Total transactions) grouped by error type or currency pair.
   * *Objective*: Distinguish user-side errors (insufficient funds) from system-side errors (insufficient liquidity, API timeouts) to monitor exchange health.

4. **Slippage and Execution Cost Metrics**:
   * *Metric*: Execution Rate Spread (percentage deviation between execution exchange rate and reference rates in `stg_rates`).
   * *Objective*: Monitor the health of transaction matching and routing engines to minimize slippage costs for users.
