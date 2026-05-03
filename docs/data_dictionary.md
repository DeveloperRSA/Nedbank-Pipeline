# Data Dictionary — DE Track Source Files

**Challenge:** Nedbank Data & Analytics Challenge — Data Engineering Track
**Document version:** 1.0

---

## 1. Overview

### Source files

Three source files are provided:

| File | Format | Row Count | Owner |
|---|---|---|---|
| `customers.csv` | CSV (comma-delimited, UTF-8) | ~80,000 | Bank (on-premises CRM export) |
| `accounts.csv` | CSV (comma-delimited, UTF-8) | ~100,000 | Fintech (daily batch extract) |
| `transactions.jsonl` | JSONL (newline-delimited JSON, UTF-8) | ~1,000,000 | Fintech (daily batch extract) |

### Business context

A major South African retail bank has partnered with a fintech to serve the unbanked and underbanked market. The fintech operates the virtual accounts and processes transactions. The bank holds the underlying customer identity and risk data. The bank's data engineering team (you) receives data from both owners and must build a clean, queryable, auditable gold layer for analytics, AI/ML, and compliance consumers.

The three files represent the three data owners:

- `customers.csv` — bank-owned customer master data (identity, KYC, segmentation)
- `accounts.csv` — fintech-owned virtual account records (account state, product, balances)
- `transactions.jsonl` — fintech-owned transaction events (activity ledger)

### Notes

- The provided data is clean. No data quality issues are present. Use it to build and validate your pipeline architecture.

---

## 2. customers.csv

**Primary key:** `customer_id`
**Encoding:** UTF-8
**Delimiter:** `,`
**Header row:** Yes

| Field Name | Type | Nullable | Description | Sample Values | Notes |
|---|---|---|---|---|---|
| `customer_id` | STRING | No | Primary key. UUID format. Unique per customer. | `"a1b2c3d4-..."` | Referenced by `accounts.customer_ref` |
| `id_number` | STRING | No | South African national ID number. Masked to last 4 digits for privacy. | `"**********1234"` | Not a join key — do not use for matching |
| `first_name` | STRING | No | Customer given name. Drawn from SA name pool (Zulu, Xhosa, Sotho, Tswana, Afrikaans, English, Tsonga, Venda). | `"Thabo"`, `"Liezel"`, `"James"` | |
| `last_name` | STRING | No | Customer surname. SA surname pool. | `"Dlamini"`, `"Van der Merwe"`, `"Naidoo"` | |
| `dob` | STRING | No | Date of birth. Format: `YYYY-MM-DD`. | `"1987-04-15"` | Used to derive `age_band` in Gold layer — do not copy raw `dob` to output. |
| `gender` | STRING | No | Gender identity. | `"M"`, `"F"`, `"NB"`, `"UNKNOWN"` | Distribution: M 45%, F 48%, NB 4%, UNKNOWN 3% |
| `province` | STRING | No | SA province of residence. | See province list below | Distribution is population-weighted (see note) |
| `income_band` | STRING | No | Gross monthly income classification. | `"LOW"`, `"LOWER_MIDDLE"`, `"MIDDLE"`, `"UPPER_MIDDLE"`, `"HIGH"` | Distribution: LOW 25%, LOWER_MIDDLE 30%, MIDDLE 25%, UPPER_MIDDLE 15%, HIGH 5% |
| `segment` | STRING | No | Nedbank customer segment. | `"MASS"`, `"EMERGING"`, `"MIDDLE_MKT"`, `"PROFESSIONAL"`, `"PRIVATE"` | Distribution: MASS 35%, EMERGING 30%, MIDDLE_MKT 20%, PROFESSIONAL 10%, PRIVATE 5% |
| `risk_score` | INTEGER | No | Risk score assigned to customer. | `1` through `10` | Normally distributed: mean 5, std 2. Clamped to [1, 10]. |
| `kyc_status` | STRING | No | Know Your Customer verification status. | `"VERIFIED"`, `"PENDING"`, `"FAILED"` | Distribution: VERIFIED 85%, PENDING 10%, FAILED 5% |
| `product_flags` | STRING | No | Pipe-delimited list of product codes held by the customer. | `"HL\|CC"`, `"SA\|CA\|INS"`, `"PL"` | Product codes: `HL` (Home Loan), `PL` (Personal Loan), `CC` (Credit Card), `SA` (Savings Account), `CA` (Current Account), `INV` (Investment), `INS` (Insurance) |

**Province values and approximate distribution:**

| Province | Approx. Share |
|---|---|
| Gauteng | 30% |
| Western Cape | 18% |
| KwaZulu-Natal | 15% |
| Eastern Cape | 10% |
| Limpopo | 8% |
| Mpumalanga | 6% |
| North West | 5% |
| Free State | 5% |
| Northern Cape | 3% |

---

## 3. accounts.csv

**Primary key:** `account_id`
**Foreign key:** `customer_ref` → `customers.customer_id`
**Encoding:** UTF-8
**Delimiter:** `,`
**Header row:** Yes

| Field Name | Type | Nullable | Description | Sample Values | Notes |
|---|---|---|---|---|---|
| `account_id` | STRING | No | Primary key. UUID format. Unique per account. | `"f9e8d7c6-..."` | |
| `customer_ref` | STRING | No | Foreign key to `customers.customer_id`. Links the account to its owner. | `"a1b2c3d4-..."` | Renamed to `customer_id` in the Gold `dim_accounts` table. |
| `account_type` | STRING | No | Type of virtual account. | `"SAVINGS"`, `"TRANSACTIONAL"`, `"CREDIT"` | Distribution: SAVINGS 40%, TRANSACTIONAL 45%, CREDIT 15% |
| `account_status` | STRING | No | Current lifecycle state of the account. | `"ACTIVE"`, `"DORMANT"`, `"CLOSED"`, `"SUSPENDED"` | Distribution: ACTIVE 70%, DORMANT 15%, CLOSED 10%, SUSPENDED 5% |
| `open_date` | STRING | No | Date the account was opened. Format: `YYYY-MM-DD`. | `"2021-06-14"` | Range: 2018-01-01 to 2025-12-31. |
| `product_tier` | STRING | No | Product tier assigned to the account. | `"BASIC"`, `"STANDARD"`, `"PREMIUM"` | Distribution: BASIC 50%, STANDARD 35%, PREMIUM 15% |
| `mobile_number` | STRING | Yes | SA mobile number associated with the account. | `"+27821234567"` | Null in ~15% of records. |
| `digital_channel` | STRING | No | Primary digital channel registered for the account. | `"APP"`, `"USSD"`, `"WEB"` | Distribution: APP 60%, USSD 30%, WEB 10% |
| `credit_limit` | DECIMAL | Yes | Approved credit limit. | `"15000.00"`, `"75000.00"` | Null for `SAVINGS` and `TRANSACTIONAL` account types. Range: 5,000–250,000 ZAR for `CREDIT` accounts. |
| `current_balance` | DECIMAL | No | Snapshot balance at time of batch extract. Two decimal places. | `"4231.50"`, `"0.00"` | Reflects balance at extract time, not real-time. |
| `last_activity_date` | STRING | Yes | Date of the most recent transaction on the account. Format: `YYYY-MM-DD`. | `"2025-11-03"` | |

**Cardinality note:** Approximately 1.25 accounts per customer on average. Distribution: 40% of customers have 1 account, 35% have 2, 20% have 3, 5% have 4.

---

## 4. transactions.jsonl

**Format:** JSONL — one complete JSON object per line. Each line is independently parseable.
**Primary key:** `transaction_id`
**Foreign key:** `account_id` → `accounts.account_id`

Each line contains a single transaction event. The top-level object has the following fields:

| Field Name | Type | Nullable | Description | Sample Values | Notes |
|---|---|---|---|---|---|
| `transaction_id` | STRING | No | Primary key. UUID format. Unique per transaction event. | `"3c7f1a2b-..."` | |
| `account_id` | STRING | No | Foreign key to `accounts.account_id`. | `"f9e8d7c6-..."` | |
| `transaction_date` | STRING | No | Date of the transaction. Format: `YYYY-MM-DD`. | `"2025-03-22"` | Range: 2024-01-01 to 2025-12-31. |
| `transaction_time` | STRING | No | Time of the transaction. Format: `HH:MM:SS`. | `"14:37:05"` | Combine with `transaction_date` to produce `transaction_timestamp` in Gold layer. |
| `transaction_type` | STRING | No | Classification of the transaction. | `"DEBIT"`, `"CREDIT"`, `"FEE"`, `"REVERSAL"` | Distribution: DEBIT 55%, CREDIT 30%, FEE 10%, REVERSAL 5% |
| `merchant_category` | STRING | Yes | MCC-style merchant category code. | `"GROCERY"`, `"FUEL"`, `"SALARY"`, `"ATM_WITHDRAWAL"` | 20 possible values — see full list below. Nullable where absent in source. |
| `amount` | DECIMAL | No | Transaction amount in ZAR. Two decimal places. | `"349.50"`, `"12000.00"` | Log-normal distribution: median ~350, range 0.01–50,000. |

| `currency` | STRING | No | Transaction currency. Always `"ZAR"`. | `"ZAR"` | |
| `channel` | STRING | No | Transaction channel. | `"POS"`, `"APP"`, `"ATM"`, `"EFT"`, `"USSD"`, `"INTERNAL"` | Distribution: POS 35%, APP 30%, ATM 15%, EFT 10%, USSD 8%, INTERNAL 2% |
| `location.province` | STRING | Yes | SA province where the transaction occurred. Nested under `location` object. | `"Gauteng"`, `"Western Cape"` | Should match the province of the customer linked to the account. |
| `location.city` | STRING | Yes | City where the transaction occurred. Nested under `location` object. | `"Johannesburg"`, `"Cape Town"`, `"Durban"` | See province-to-city mapping in notes below. |
| `location.coordinates` | STRING | Yes | Approximate lat/lon. Nested under `location` object. | `"-26.2041,28.0473"` | Null in ~40% of records. |
| `metadata.device_id` | STRING | Yes | Device identifier for digital channel transactions. Nested under `metadata` object. | `"dev-a9f3..."` | Null in ~50% of records. |
| `metadata.session_id` | STRING | Yes | Session identifier. Nested under `metadata` object. | `"sess-7b12..."` | Null in ~40% of records. |
| `metadata.retry_flag` | BOOLEAN | No | Whether this event was a retry of a previously failed submission. Nested under `metadata` object. | `false`, `true` | True in ~2% of records. |

**Example record (Stage 1):**

```json
{
  "transaction_id": "3c7f1a2b-e4d5-4f6a-8b9c-0d1e2f3a4b5c",
  "account_id": "f9e8d7c6-b5a4-4321-9876-543210fedcba",
  "transaction_date": "2025-03-22",
  "transaction_time": "14:37:05",
  "transaction_type": "DEBIT",
  "merchant_category": "GROCERY",
  "amount": 349.50,
  "currency": "ZAR",
  "channel": "POS",
  "location": {
    "province": "Gauteng",
    "city": "Sandton",
    "coordinates": "-26.1076,28.0567"
  },
  "metadata": {
    "device_id": null,
    "session_id": null,
    "retry_flag": false
  }
}
```

**Merchant category values (20 total):**

`GROCERY`, `FUEL`, `RESTAURANT`, `RETAIL`, `HEALTHCARE`, `UTILITIES`, `TRANSPORT`, `ENTERTAINMENT`, `EDUCATION`, `INSURANCE`, `RENT`, `SALARY`, `ATM_WITHDRAWAL`, `TRANSFER_IN`, `TRANSFER_OUT`, `REVERSAL_CREDIT`, `REVERSAL_DEBIT`, `FEE_SERVICE`, `FEE_MONTHLY`, `FEE_TRANSACTION`

---

## 5. Relationships

```
customers.csv                accounts.csv              transactions.jsonl
─────────────────            ─────────────────         ──────────────────────
customer_id (PK) ────────── customer_ref (FK)
                             account_id (PK) ─────── account_id (FK)
```

**Cardinality:**

| Relationship | Cardinality | Detail |
|---|---|---|
| customers → accounts | 1 : 1..4 | Each customer has 1–4 accounts. Average ~1.25 accounts per customer. |
| accounts → transactions | 1 : 0..N | Active accounts generate the bulk of transactions. Dormant/Closed accounts have proportionally fewer. |
| Effective depth | Customer → ~1.25 accounts → ~10 transactions per account | Approximately 12.5 transactions per customer. |

**Gold layer join path:**

- `fact_transactions.account_sk` → `dim_accounts.account_sk` (via `transactions.account_id` → `accounts.account_id`)
- `fact_transactions.customer_sk` → `dim_customers.customer_sk` (via `dim_accounts.customer_id` → `dim_customers.customer_id`)
- `dim_accounts.customer_id` is renamed from `accounts.customer_ref` in the Gold layer

---

*End of document.*
