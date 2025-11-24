CREATE DATABASE banking_transaction_monitoring_system;
USE banking_transaction_monitoring_system;

-- 1. Create table : Customers
CREATE TABLE customers (
  customer_id INT AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email VARCHAR(255) UNIQUE,
  phone VARCHAR(20),
  date_of_birth DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 2. Branches
CREATE TABLE branches (
  branch_id INT AUTO_INCREMENT PRIMARY KEY,
  branch_code VARCHAR(20) UNIQUE,
  name VARCHAR(255),
  city VARCHAR(100),
  state VARCHAR(100),
  country VARCHAR(100),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 3. Accounts
CREATE TABLE accounts (
  account_id BIGINT PRIMARY KEY,
  customer_id INT NOT NULL,
  account_number VARCHAR(30) UNIQUE,
  account_type ENUM('savings','current','salary','loan','credit') DEFAULT 'savings',
  currency CHAR(3) DEFAULT 'INR',
  opened_date DATE,
  status ENUM('active','closed','frozen') DEFAULT 'active',
  branch_id INT,
  balance DECIMAL(18,2) DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
  FOREIGN KEY (branch_id) REFERENCES branches(branch_id)
) ENGINE=InnoDB;

CREATE INDEX idx_accounts_customer ON accounts(customer_id);
CREATE INDEX idx_accounts_status ON accounts(status);

-- 4. Cards
CREATE TABLE cards (
  card_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  card_number_hash CHAR(64) NOT NULL, -- store hashed/tokenized PAN
  card_type ENUM('debit','credit') DEFAULT 'debit',
  scheme VARCHAR(50), -- VISA/MASTERCARD/RUPAY
  expiry_date DATE,
  status ENUM('active','blocked','expired') DEFAULT 'active',
  issued_at DATE,
  FOREIGN KEY (account_id) REFERENCES accounts(account_id)
) ENGINE=InnoDB;

CREATE INDEX idx_cards_account ON cards(account_id);
CREATE INDEX idx_cards_scheme ON cards(scheme);

-- 5. Merchants (POS / online)
CREATE TABLE merchants (
  merchant_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  merchant_name VARCHAR(255),
  merchant_category_code VARCHAR(10),
  city VARCHAR(100),
  country VARCHAR(100),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 6. Transactions (central table)
-- Note: this table will grow large. Use proper indexing and consider partitioning by range(order_date).
CREATE TABLE transactions (
  transaction_id BIGINT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  related_account_id BIGINT DEFAULT NULL, -- for transfers
  card_id BIGINT DEFAULT NULL,            -- if card involved
  merchant_id BIGINT DEFAULT NULL,        -- if merchant involved
  branch_id INT DEFAULT NULL,             -- branch where txn happened (if any)
  transaction_type ENUM('debit','credit','transfer','fee','refund','cash_withdrawal','deposit','payment') NOT NULL,
  amount DECIMAL(18,2) NOT NULL,
  currency CHAR(3) DEFAULT 'INR',
  order_id VARCHAR(100) DEFAULT NULL,     -- external payment order id (if any)
  status ENUM('success','failed','pending','reversed') DEFAULT 'success',
  channel ENUM('atm','branch','pos','netbanking','upi','mobile','ivr','api') DEFAULT 'mobile',
  txn_timestamp DATETIME NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  merchant_city VARCHAR(100) DEFAULT NULL,
  merchant_country VARCHAR(100) DEFAULT NULL,
  txn_raw_json JSON DEFAULT NULL,         -- raw payload for forensic analysis
  INDEX idx_transactions_account_date (account_id, txn_timestamp),
  INDEX idx_transactions_date (txn_timestamp),
  INDEX idx_transactions_type_status (transaction_type, status),
  FOREIGN KEY (account_id) REFERENCES accounts(account_id),
  FOREIGN KEY (card_id) REFERENCES cards(card_id),
  FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id),
  FOREIGN KEY (branch_id) REFERENCES branches(branch_id)
) ENGINE=InnoDB;


-- 7. Logins (authentication events) - useful to correlate device/IP anomalies
CREATE TABLE logins (
  login_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  account_id BIGINT DEFAULT NULL,
  login_time DATETIME NOT NULL,
  device_info VARCHAR(512) DEFAULT NULL,
  ip_address VARCHAR(45) DEFAULT NULL,
  success BOOLEAN DEFAULT TRUE,
  location VARCHAR(255) DEFAULT NULL,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

CREATE INDEX idx_logins_customer_time ON logins(customer_id, login_time);

-- 8. Fraud Alerts / Investigations
CREATE TABLE fraud_alerts (
  alert_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  transaction_id BIGINT DEFAULT NULL,
  account_id BIGINT DEFAULT NULL,
  alert_type VARCHAR(100),     -- e.g., suspicious_amount, geo_anomaly, velocity
  alert_score DECIMAL(5,2),    -- numeric score from rules/ML
  alert_status ENUM('open','investigating','closed','false_positive') DEFAULT 'open',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  closed_at TIMESTAMP NULL,
  investigator VARCHAR(255) DEFAULT NULL,
  notes TEXT,
  FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
  FOREIGN KEY (account_id) REFERENCES accounts(account_id)
) ENGINE=InnoDB;

CREATE INDEX idx_fraud_account ON fraud_alerts(account_id);
CREATE INDEX idx_fraud_status ON fraud_alerts(alert_status);

-- 9. Transfers (denormalized view / optional for quick reporting)
CREATE TABLE transfers (
  transfer_id BIGINT PRIMARY KEY,
  from_account BIGINT NOT NULL,
  to_account BIGINT NOT NULL,
  amount DECIMAL(18,2) NOT NULL,
  transfer_timestamp DATETIME NOT NULL,
  status ENUM('success','failed','pending') DEFAULT 'success',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE INDEX idx_transfers_from_time ON transfers(from_account, transfer_timestamp);

-- 10. Transaction Labels (for model training / ground truth)
CREATE TABLE transaction_labels (
  transaction_id BIGINT PRIMARY KEY,
  is_fraud BOOLEAN DEFAULT FALSE,
  label_source ENUM('investigation','manual','model') DEFAULT 'manual',
  labeled_at TIMESTAMP NULL,
  FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id)
) ENGINE=InnoDB;
-- ================================================================================== --
select * from customers;

-- =====================================================================================
select * from transactions;
select * from accounts;
select * from cards;
select * from fraud_alerts;
select * from logins;
select * from merchants;
select * from transaction_labels;
select * from transactions;
select * from transfers;
SHOW COLUMNS FROM transfers;

-- ============================================================
SELECT * FROM customers LIMIT 10;
SELECT * FROM accounts LIMIT 10;
SELECT * FROM merchants LIMIT 10;
SELECT * FROM cards LIMIT 10;
SELECT * FROM transactions LIMIT 10;
SELECT * FROM logins LIMIT 10;
SELECT * FROM fraud_alerts LIMIT 10;
SELECT * FROM transaction_labels LIMIT 10;

-- Customers + Accounts
SELECT 
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    accounts.account_id,
    accounts.account_type,
    accounts.balance
FROM customers
INNER JOIN accounts
    ON customers.customer_id = accounts.customer_id
LIMIT 20;



-- Accounts + Transactions
SELECT
    accounts.account_id,
    accounts.customer_id,
    transactions.transaction_id,
    transactions.transaction_type,
    transactions.amount,
    transactions.status,
    transactions.txn_timestamp
FROM accounts
INNER JOIN transactions
    ON accounts.account_id = transactions.account_id
ORDER BY transactions.txn_timestamp DESC
LIMIT 20;
-- Transactions + Merchants
SELECT
    transactions.transaction_id,
    transactions.amount,
    transactions.transaction_type,
    merchants.merchant_id,
    merchants.merchant_name,
    merchants.city,
    merchants.country
FROM transactions
LEFT JOIN merchants
    ON transactions.merchant_id = merchants.merchant_id
LIMIT 20;
-- Transactions + Cards 
SELECT
    transactions.transaction_id,
    transactions.amount,
    cards.card_id,
    cards.card_type,
    cards.scheme,
    cards.card_number_hash
FROM transactions
LEFT JOIN cards
    ON transactions.card_id = cards.card_id
LIMIT 20;
-- Logins + Customers + Accounts (Full Form Join)
SELECT
    logins.login_id,
    logins.customer_id,
    logins.login_time,
    logins.ip_address,
    customers.first_name,
    customers.last_name,
    accounts.account_id,
    accounts.account_type
FROM logins
INNER JOIN customers
    ON logins.customer_id = customers.customer_id
LEFT JOIN accounts
    ON logins.customer_id = accounts.customer_id
ORDER BY logins.login_time DESC
LIMIT 30;
-- Fraud Alerts + Transactions + Accounts (Full Form Join)
SELECT
    fraud_alerts.alert_id,
    fraud_alerts.alert_type,
    fraud_alerts.alert_score,
    fraud_alerts.alert_status,
    fraud_alerts.created_at,
    transactions.transaction_id,
    transactions.amount,
    transactions.transaction_type,
    accounts.account_id,
    accounts.customer_id
FROM fraud_alerts
LEFT JOIN transactions
    ON fraud_alerts.transaction_id = transactions.transaction_id
LEFT JOIN accounts
    ON fraud_alerts.account_id = accounts.account_id
ORDER BY fraud_alerts.alert_score DESC
LIMIT 30;

/* ======================================================================================
	KPIs
====================================================================================== */

----------------------------------------------------------------------------------------
-- 1. Daily total transactions
----------------------------------------------------------------------------------------
SELECT 
    DATE(transactions.txn_timestamp) AS transaction_date,
    COUNT(*) AS total_transactions
FROM transactions
GROUP BY DATE(transactions.txn_timestamp)
ORDER BY transaction_date;

----------------------------------------------------------------------------------------
-- 2. Total debit vs credit count
----------------------------------------------------------------------------------------
SELECT
    transactions.transaction_type,
    COUNT(*) AS total_count
FROM transactions
GROUP BY transactions.transaction_type;

----------------------------------------------------------------------------------------
-- 3. Total revenue (sum of all debits)
----------------------------------------------------------------------------------------
SELECT
    SUM(transactions.amount) AS total_revenue
FROM transactions
WHERE transactions.transaction_type = 'debit';

----------------------------------------------------------------------------------------
-- 4. Average transaction amount per customer
----------------------------------------------------------------------------------------
SELECT
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    AVG(transactions.amount) AS avg_amount
FROM customers
INNER JOIN accounts
    ON customers.customer_id = accounts.customer_id
INNER JOIN transactions
    ON accounts.account_id = transactions.account_id
GROUP BY 
    customers.customer_id,
    customers.first_name,
    customers.last_name;

----------------------------------------------------------------------------------------
-- 5. Top 10 highest spending customers (debit)
----------------------------------------------------------------------------------------
SELECT
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    SUM(transactions.amount) AS total_spent
FROM customers
INNER JOIN accounts
    ON customers.customer_id = accounts.customer_id
INNER JOIN transactions
    ON accounts.account_id = transactions.account_id
WHERE transactions.transaction_type = 'debit'
GROUP BY 
    customers.customer_id,
    customers.first_name,
    customers.last_name
ORDER BY total_spent DESC
LIMIT 10;

----------------------------------------------------------------------------------------
-- 6. Merchant-wise total revenue
----------------------------------------------------------------------------------------
SELECT
    merchants.merchant_id,
    merchants.merchant_name,
    SUM(transactions.amount) AS total_revenue
FROM merchants
INNER JOIN transactions
    ON merchants.merchant_id = transactions.merchant_id
GROUP BY merchants.merchant_id, merchants.merchant_name
ORDER BY total_revenue DESC;

----------------------------------------------------------------------------------------
-- 7. Customer-wise total number of transactions
----------------------------------------------------------------------------------------
SELECT
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    COUNT(transactions.transaction_id) AS total_transactions
FROM customers
INNER JOIN accounts
    ON customers.customer_id = accounts.customer_id
INNER JOIN transactions
    ON accounts.account_id = transactions.account_id
GROUP BY 
    customers.customer_id,
    customers.first_name,
    customers.last_name;

----------------------------------------------------------------------------------------
-- 8. Channel-wise transaction volume (ATM/UPI/etc.)
----------------------------------------------------------------------------------------
SELECT
    transactions.channel,
    COUNT(*) AS total_transactions
FROM transactions
GROUP BY transactions.channel;

----------------------------------------------------------------------------------------
-- 9. City-wise merchant revenue
----------------------------------------------------------------------------------------
SELECT
    merchants.city,
    SUM(transactions.amount) AS total_amount
FROM merchants
INNER JOIN transactions
    ON merchants.merchant_id = transactions.merchant_id
GROUP BY merchants.city
ORDER BY total_amount DESC;

----------------------------------------------------------------------------------------
-- 10. Success vs Failed transactions
----------------------------------------------------------------------------------------
SELECT
    transactions.status,
    COUNT(*) AS total_count
FROM transactions
GROUP BY transactions.status;

----------------------------------------------------------------------------------------
-- 11. Average balance per account type
----------------------------------------------------------------------------------------
SELECT
    accounts.account_type,
    AVG(accounts.balance) AS avg_balance
FROM accounts
GROUP BY accounts.account_type;

----------------------------------------------------------------------------------------
-- 12. Top 10 merchants by number of transactions
----------------------------------------------------------------------------------------
SELECT
    merchants.merchant_id,
    merchants.merchant_name,
    COUNT(transactions.transaction_id) AS txn_count
FROM merchants
INNER JOIN transactions
    ON merchants.merchant_id = transactions.merchant_id
GROUP BY merchants.merchant_id, merchants.merchant_name
ORDER BY txn_count DESC
LIMIT 10;

----------------------------------------------------------------------------------------
-- 13. Monthly transactions
----------------------------------------------------------------------------------------
SELECT
    DATE_FORMAT(transactions.txn_timestamp, '%Y-%m') AS month,
    COUNT(*) AS total
FROM transactions
GROUP BY DATE_FORMAT(transactions.txn_timestamp, '%Y-%m')
ORDER BY month;

----------------------------------------------------------------------------------------
-- 14. Daily average transaction amount
----------------------------------------------------------------------------------------
SELECT
    DATE(transactions.txn_timestamp) AS txn_date,
    AVG(transactions.amount) AS avg_txn_amount
FROM transactions
GROUP BY DATE(transactions.txn_timestamp);

----------------------------------------------------------------------------------------
-- 15. Account-wise total incoming (credits)
----------------------------------------------------------------------------------------
SELECT
    accounts.account_id,
    SUM(transactions.amount) AS total_credit
FROM accounts
INNER JOIN transactions
    ON accounts.account_id = transactions.account_id
WHERE transactions.transaction_type = 'credit'
GROUP BY accounts.account_id
ORDER BY total_credit DESC;

----------------------------------------------------------------------------------------
-- 16. Account-wise total outgoing (debits)
----------------------------------------------------------------------------------------
SELECT
    accounts.account_id,
    SUM(transactions.amount) AS total_debit
FROM accounts
INNER JOIN transactions
    ON accounts.account_id = transactions.account_id
WHERE transactions.transaction_type = 'debit'
GROUP BY accounts.account_id
ORDER BY total_debit DESC;

----------------------------------------------------------------------------------------
-- 17. Branch-wise total balance
----------------------------------------------------------------------------------------
SELECT
    branches.branch_id,
    branches.branch_name,
    SUM(accounts.balance) AS total_balance
FROM branches
INNER JOIN accounts
    ON branches.branch_id = accounts.branch_id
GROUP BY branches.branch_id, branches.branch_name;

----------------------------------------------------------------------------------------
-- 18. Customer with multiple cards
----------------------------------------------------------------------------------------
SELECT
    accounts.customer_id,
    COUNT(cards.card_id) AS total_cards
FROM accounts
INNER JOIN cards
    ON accounts.account_id = cards.account_id
GROUP BY accounts.customer_id
HAVING total_cards > 1;

----------------------------------------------------------------------------------------
-- 19. Top 10 high value transactions
----------------------------------------------------------------------------------------
SELECT
    transactions.transaction_id,
    transactions.amount,
    transactions.txn_timestamp
FROM transactions
ORDER BY transactions.amount DESC
LIMIT 10;

----------------------------------------------------------------------------------------
-- 20. Average login attempts per customer
----------------------------------------------------------------------------------------
SELECT
    customers.customer_id,
    COUNT(logins.login_id) AS total_logins
FROM customers
INNER JOIN logins
    ON customers.customer_id = logins.customer_id
GROUP BY customers.customer_id;


/* ======================================================================================
    PART–B : 25 ADVANCED FRAUD ANALYTICS RULES (FULL FORM, NO ALIAS)
====================================================================================== */

----------------------------------------------------------------------------------------
-- 1. High Velocity: More than 10 transactions within 5 minutes
----------------------------------------------------------------------------------------
SELECT
    transactions.account_id,
    COUNT(*) AS txn_count,
    MIN(transactions.txn_timestamp) AS window_start,
    MAX(transactions.txn_timestamp) AS window_end
FROM transactions
GROUP BY transactions.account_id,
         DATE_FORMAT(transactions.txn_timestamp, '%Y-%m-%d %H:%i')
HAVING txn_count > 10;

----------------------------------------------------------------------------------------
-- 2. Large amount anomaly (above 50,000)
----------------------------------------------------------------------------------------
SELECT *
FROM transactions
WHERE transactions.amount > 50000;

----------------------------------------------------------------------------------------
-- 3. Suspicious late night activity (12am–4am)
----------------------------------------------------------------------------------------
SELECT *
FROM transactions
WHERE HOUR(transactions.txn_timestamp) BETWEEN 0 AND 4;

----------------------------------------------------------------------------------------
-- 4. Multiple IP addresses used by same customer
----------------------------------------------------------------------------------------
SELECT
    logins.customer_id,
    COUNT(DISTINCT logins.ip_address) AS ip_count
FROM logins
GROUP BY logins.customer_id
HAVING ip_count > 5;

----------------------------------------------------------------------------------------
-- 5. Multiple devices used by same customer
----------------------------------------------------------------------------------------
SELECT
    logins.customer_id,
    COUNT(DISTINCT logins.device_info) AS device_count
FROM logins
GROUP BY logins.customer_id
HAVING device_count > 3;

----------------------------------------------------------------------------------------
-- 6. Same IP used to access multiple accounts
----------------------------------------------------------------------------------------
SELECT
    logins.ip_address,
    COUNT(DISTINCT logins.customer_id) AS different_customers
FROM logins
GROUP BY logins.ip_address
HAVING different_customers > 3;

----------------------------------------------------------------------------------------
-- 7. High risk merchants (MCC = cash-like categories)
----------------------------------------------------------------------------------------
SELECT *
FROM merchants
WHERE merchants.merchant_category_code IN ('5411','5814','4111');

----------------------------------------------------------------------------------------
-- 8. Customer with too many failed transactions
----------------------------------------------------------------------------------------
SELECT
    transactions.account_id,
    COUNT(*) AS failed_txns
FROM transactions
WHERE transactions.status = 'failed'
GROUP BY transactions.account_id
HAVING failed_txns > 5;

----------------------------------------------------------------------------------------
-- 9. Unusually high balance drop in 1 day
----------------------------------------------------------------------------------------
SELECT
    accounts.account_id,
    SUM(transactions.amount) AS total_out
FROM accounts
INNER JOIN transactions
    ON accounts.account_id = transactions.account_id
WHERE transactions.transaction_type = 'debit'
GROUP BY accounts.account_id
HAVING total_out > 100000;

----------------------------------------------------------------------------------------
-- 10. Repeated small transactions (structuring)
----------------------------------------------------------------------------------------
SELECT
    transactions.account_id,
    COUNT(*) AS txn_count
FROM transactions
WHERE transactions.amount < 500
GROUP BY transactions.account_id
HAVING txn_count > 20;

----------------------------------------------------------------------------------------
-- 11. Multiple countries in same day
----------------------------------------------------------------------------------------
SELECT
    transactions.account_id,
    COUNT(DISTINCT transactions.merchant_country) AS country_count
FROM transactions
GROUP BY transactions.account_id
HAVING country_count > 2;

----------------------------------------------------------------------------------------
-- 12. Card used at two far locations within short time
----------------------------------------------------------------------------------------
SELECT
    card_id,
    MIN(txn_timestamp) AS first_txn,
    MAX(txn_timestamp) AS last_txn
FROM transactions
GROUP BY card_id
HAVING TIMESTAMPDIFF(MINUTE, MIN(txn_timestamp), MAX(txn_timestamp)) < 15;

----------------------------------------------------------------------------------------
-- 13. Too many login failures
----------------------------------------------------------------------------------------
SELECT
    logins.customer_id,
    COUNT(*) AS failed_attempts
FROM logins
WHERE logins.success = 0
GROUP BY logins.customer_id
HAVING failed_attempts > 5;

----------------------------------------------------------------------------------------
-- 14. Same card used by multiple accounts (token misuse)
----------------------------------------------------------------------------------------
SELECT
    cards.card_number_hash,
    COUNT(DISTINCT cards.account_id) AS linked_accounts
FROM cards
GROUP BY cards.card_number_hash
HAVING linked_accounts > 1;

----------------------------------------------------------------------------------------
-- 15. High merchant exposure (too many customers)
----------------------------------------------------------------------------------------
SELECT
    transactions.merchant_id,
    COUNT(DISTINCT accounts.customer_id) AS unique_customers
FROM transactions
INNER JOIN accounts
    ON transactions.account_id = accounts.account_id
GROUP BY transactions.merchant_id
HAVING unique_customers > 50;

----------------------------------------------------------------------------------------
-- 16. Unusual refund patterns
----------------------------------------------------------------------------------------
SELECT *
FROM transactions
WHERE transactions.transaction_type = 'refund'
ORDER BY transactions.amount DESC;

----------------------------------------------------------------------------------------
-- 17. Same customer accessing multiple accounts (account takeover)
----------------------------------------------------------------------------------------
SELECT
    logins.customer_id,
    COUNT(DISTINCT logins.account_id) AS total_accounts
FROM logins
GROUP BY logins.customer_id
HAVING total_accounts > 1;

----------------------------------------------------------------------------------------
-- 18. High number of disputed transactions (fraud alerts)
----------------------------------------------------------------------------------------
SELECT
    fraud_alerts.account_id,
    COUNT(*) AS total_alerts
FROM fraud_alerts
GROUP BY fraud_alerts.account_id
HAVING total_alerts > 5;

----------------------------------------------------------------------------------------
-- 19. Stolen card pattern (transactions in multiple states)
----------------------------------------------------------------------------------------
SELECT
    transactions.card_id,
    COUNT(DISTINCT merchants.city) AS different_cities
FROM transactions
LEFT JOIN merchants
    ON transactions.merchant_id = merchants.merchant_id
GROUP BY transactions.card_id
HAVING different_cities > 5;

----------------------------------------------------------------------------------------
-- 20. Same account making too many transfers
----------------------------------------------------------------------------------------
SELECT
    transfers.from_account,
    COUNT(*) AS transfer_count
FROM transfers
GROUP BY transfers.from_account
HAVING transfer_count > 10;

----------------------------------------------------------------------------------------
-- 21. Too many ATM cash withdrawals
----------------------------------------------------------------------------------------
SELECT
    transactions.account_id,
    COUNT(*) AS cash_withdrawals
FROM transactions
WHERE transactions.transaction_type = 'cash_withdrawal'
GROUP BY transactions.account_id
HAVING cash_withdrawals > 5;

----------------------------------------------------------------------------------------
-- 22. Sudden change in transaction behavior (spike)
----------------------------------------------------------------------------------------
SELECT
    transactions.account_id,
    AVG(transactions.amount) AS avg_amount
FROM transactions
GROUP BY transactions.account_id
HAVING avg_amount > 20000;

----------------------------------------------------------------------------------------
-- 23. High-risk transaction locations
----------------------------------------------------------------------------------------
SELECT *
FROM transactions
WHERE transactions.merchant_city IN ('Unknown','Outside India');

----------------------------------------------------------------------------------------
-- 24. Very frequent logins in short span
----------------------------------------------------------------------------------------
SELECT
    logins.customer_id,
    COUNT(*) AS total_logins
FROM logins
GROUP BY logins.customer_id
HAVING total_logins > 20;

----------------------------------------------------------------------------------------
-- 25. Multiple refunds in short time
----------------------------------------------------------------------------------------
SELECT
    transactions.account_id,
    COUNT(*) AS refund_count
FROM transactions
WHERE transactions.transaction_type = 'refund'
GROUP BY transactions.account_id
HAVING refund_count > 3;



