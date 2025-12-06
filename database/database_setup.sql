DROP DATABASE IF EXISTS equity_research;
CREATE DATABASE equity_research;
USE equity_research;


-- 1. Sector Table
CREATE TABLE Sectors (
                         sector_id INT PRIMARY KEY,
                         sector_name VARCHAR(100) NOT NULL UNIQUE,
                         industry_category VARCHAR(100),
                         sector_index_ticker VARCHAR(20),
                         INDEX idx_sector_name (sector_name)
);

CREATE TABLE Permission (
                            permission_level INT PRIMARY KEY,
                            level_name VARCHAR(50) NOT NULL UNIQUE,
                            level_description TEXT,
                            can_create BOOLEAN DEFAULT FALSE,
                            can_read BOOLEAN DEFAULT TRUE,
                            can_update BOOLEAN DEFAULT FALSE,
                            can_delete BOOLEAN DEFAULT FALSE,
                            can_execute_reports BOOLEAN DEFAULT FALSE,
                            can_manage_users BOOLEAN DEFAULT FALSE,
                            can_approve BOOLEAN DEFAULT FALSE,

                            CONSTRAINT chk_permission_level CHECK (permission_level >= 1 AND permission_level <= 10),
                            INDEX idx_level_name (level_name)
);

CREATE TABLE Role (
                      role_id INT AUTO_INCREMENT PRIMARY KEY,
                      role_name VARCHAR(50) NOT NULL UNIQUE,
                      role_description TEXT,
                      permission_level INT NOT NULL,
                      is_active BOOLEAN DEFAULT TRUE,
                      CONSTRAINT fk_role_permission
                          FOREIGN KEY (permission_level) REFERENCES Permission(permission_level)
                              ON DELETE RESTRICT
                              ON UPDATE CASCADE,

                      INDEX idx_role_name (role_name),
                      INDEX idx_permission_level (permission_level)
);

CREATE TABLE Department (
                            department_id INT AUTO_INCREMENT PRIMARY KEY,
                            department_name VARCHAR(100) NOT NULL UNIQUE,
                            INDEX idx_department_name (department_name)
);


CREATE TABLE Users (
                       user_id INT AUTO_INCREMENT PRIMARY KEY,
                       username VARCHAR(50) NOT NULL UNIQUE,
                       email VARCHAR(100) NOT NULL UNIQUE,
                       password_hash VARCHAR(255) NOT NULL,
                       full_name VARCHAR(100),
                       role_id INT NOT NULL,
                       department_id INT,
                       phone_number VARCHAR(20),
                       created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                       last_login TIMESTAMP NULL,
                       is_active BOOLEAN DEFAULT TRUE,

                       days_since_last_login INT,

                       CONSTRAINT fk_user_role
                           FOREIGN KEY (role_id) REFERENCES Role(role_id)
                               ON DELETE RESTRICT
                               ON UPDATE CASCADE,

                       CONSTRAINT fk_user_department
                           FOREIGN KEY (department_id) REFERENCES Department(department_id)
                               ON DELETE SET NULL  -- If department deleted, set to NULL
                               ON UPDATE CASCADE,

                       INDEX idx_username (username),
                       INDEX idx_email (email),
                       INDEX idx_role_id (role_id),
                       INDEX idx_department_id (department_id)
);

CREATE TABLE Companies (
                           company_id INT PRIMARY KEY AUTO_INCREMENT,
                           ticker_symbol VARCHAR(10) UNIQUE NOT NULL,
                           company_name VARCHAR(200) NOT NULL,
                           sector_id INT NOT NULL,
                           market_cap DECIMAL(20, 2),
                           country VARCHAR(100),
                           incorporation_date DATE,
                           description TEXT,
                           exchange VARCHAR(50),
                           currency VARCHAR(3) DEFAULT 'USD',
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                           FOREIGN KEY (sector_id) REFERENCES Sectors(sector_id) ON DELETE RESTRICT,
                           INDEX idx_ticker (ticker_symbol),
                           INDEX idx_sector (sector_id),
                           INDEX idx_market_cap (market_cap)
);

-- Financial Statements (Superclass)
CREATE TABLE FinancialStatements (
                                     statement_id INT PRIMARY KEY AUTO_INCREMENT,
                                     company_id INT NOT NULL,
                                     fiscal_year YEAR NOT NULL,
                                     fiscal_quarter ENUM('Q1', 'Q2', 'Q3', 'Q4', 'FY') NOT NULL,
                                     filing_date DATE NOT NULL,
                                     statement_type ENUM('IncomeStatement', 'BalanceSheet', 'CashFlowStatement') NOT NULL,
                                     currency VARCHAR(3) DEFAULT 'USD',
                                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                     FOREIGN KEY (company_id) REFERENCES Companies(company_id) ON DELETE CASCADE,
                                     INDEX idx_company_period (company_id, fiscal_year, fiscal_quarter),
                                     INDEX idx_statement_type (statement_type),
                                     UNIQUE KEY unique_statement (company_id, fiscal_year, fiscal_quarter, statement_type)
);

-- Income Statements (Subclass)
CREATE TABLE IncomeStatements (
                                  statement_id INT PRIMARY KEY,
                                  revenue DECIMAL(20, 2),
                                  cost_of_revenue DECIMAL(20, 2),
                                  gross_profit DECIMAL(20, 2),
                                  operating_expenses DECIMAL(20, 2),
                                  operating_income DECIMAL(20, 2),
                                  interest_expense DECIMAL(20, 2),
                                  income_before_tax DECIMAL(20, 2),
                                  income_tax_expense DECIMAL(20, 2),
                                  net_income DECIMAL(20, 2),
                                  eps_basic DECIMAL(10, 4),
                                  eps_diluted DECIMAL(10, 4),
                                  shares_outstanding BIGINT,
                                  FOREIGN KEY (statement_id) REFERENCES FinancialStatements(statement_id) ON DELETE CASCADE,
                                  INDEX idx_revenue (revenue),
                                  INDEX idx_net_income (net_income)
);

-- Balance Sheets (Subclass)
CREATE TABLE BalanceSheets (
                               statement_id INT PRIMARY KEY,
                               total_assets DECIMAL(20, 2),
                               current_assets DECIMAL(20, 2),
                               cash_and_equivalents DECIMAL(20, 2),
                               accounts_receivable DECIMAL(20, 2),
                               inventory DECIMAL(20, 2),
                               non_current_assets DECIMAL(20, 2),
                               property_plant_equipment DECIMAL(20, 2),
                               total_liabilities DECIMAL(20, 2),
                               current_liabilities DECIMAL(20, 2),
                               accounts_payable DECIMAL(20, 2),
                               short_term_debt DECIMAL(20, 2),
                               long_term_debt DECIMAL(20, 2),
                               total_equity DECIMAL(20, 2),
                               retained_earnings DECIMAL(20, 2),
                               FOREIGN KEY (statement_id) REFERENCES FinancialStatements(statement_id) ON DELETE CASCADE,
                               INDEX idx_total_assets (total_assets),
                               INDEX idx_total_equity (total_equity)
);

-- Cash Flow Statements (Subclass)
CREATE TABLE CashFlowStatements (
                                    statement_id INT PRIMARY KEY,
                                    operating_cash_flow DECIMAL(20, 2),
                                    investing_cash_flow DECIMAL(20, 2),
                                    financing_cash_flow DECIMAL(20, 2),
                                    net_change_in_cash DECIMAL(20, 2),
                                    capital_expenditure DECIMAL(20, 2),
                                    free_cash_flow DECIMAL(20, 2),
                                    dividends_paid DECIMAL(20, 2),
                                    stock_repurchases DECIMAL(20, 2),
                                    FOREIGN KEY (statement_id) REFERENCES FinancialStatements(statement_id) ON DELETE CASCADE,
                                    INDEX idx_operating_cf (operating_cash_flow),
                                    INDEX idx_free_cf (free_cash_flow)
);

-- Stock Prices (Time Series Data)
CREATE TABLE StockPrices (
                             price_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                             company_id INT NOT NULL,
                             trade_date DATE NOT NULL,
                             open_price DECIMAL(12, 4),
                             high_price DECIMAL(12, 4),
                             low_price DECIMAL(12, 4),
                             close_price DECIMAL(12, 4),
                             adjusted_close DECIMAL(12, 4),
                             volume BIGINT,
                             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                             FOREIGN KEY (company_id) REFERENCES Companies(company_id) ON DELETE CASCADE,
                             UNIQUE KEY unique_price (company_id, trade_date),
                             INDEX idx_company_date (company_id, trade_date),
                             INDEX idx_trade_date (trade_date)
);

-- Valuation Metrics
CREATE TABLE ValuationMetrics (
                                  metric_id INT PRIMARY KEY AUTO_INCREMENT,
                                  company_id INT NOT NULL,
                                  calculation_date DATE NOT NULL,
                                  pe_ratio DECIMAL(10, 4),
                                  pb_ratio DECIMAL(10, 4),
                                  ps_ratio DECIMAL(10, 4),
                                  roe DECIMAL(10, 4),
                                  roa DECIMAL(10, 4),
                                  debt_to_equity DECIMAL(10, 4),
                                  current_ratio DECIMAL(10, 4),
                                  quick_ratio DECIMAL(10, 4),
                                  gross_margin DECIMAL(10, 4),
                                  operating_margin DECIMAL(10, 4),
                                  net_margin DECIMAL(10, 4),
                                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                  FOREIGN KEY (company_id) REFERENCES Companies(company_id) ON DELETE CASCADE,
                                  UNIQUE KEY unique_metric (company_id, calculation_date),
                                  INDEX idx_company_date (company_id, calculation_date),
                                  INDEX idx_pe_ratio (pe_ratio)
);

-- Forecasts (System-generated)
CREATE TABLE Forecasts (
                           forecast_id INT PRIMARY KEY AUTO_INCREMENT,
                           company_id INT NOT NULL,
                           forecast_date DATE NOT NULL,
                           target_date DATE NOT NULL,
                           target_price DECIMAL(12, 4),
                           revenue_forecast DECIMAL(20, 2),
                           eps_forecast DECIMAL(10, 4),
                           recommendation ENUM('Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell'),
                           confidence_score DECIMAL(5, 4),
                           model_version VARCHAR(50),
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           FOREIGN KEY (company_id) REFERENCES Companies(company_id) ON DELETE CASCADE,
                           INDEX idx_company_forecast (company_id, forecast_date),
                           INDEX idx_recommendation (recommendation),
                           CHECK (confidence_score BETWEEN 0 AND 1)
);

-- Create Views for Common Queries

-- Latest Stock Prices
CREATE VIEW vw_latest_stock_prices AS
SELECT
    c.ticker_symbol,
    c.company_name,
    sp.trade_date,
    sp.close_price,
    sp.volume
FROM Companies c
         JOIN StockPrices sp ON c.company_id = sp.company_id
WHERE sp.trade_date = (
    SELECT MAX(trade_date)
    FROM StockPrices
    WHERE company_id = c.company_id
);

-- Latest Valuation Metrics
CREATE VIEW vw_latest_valuations AS
SELECT
    c.ticker_symbol,
    c.company_name,
    s.sector_name,
    vm.calculation_date,
    vm.pe_ratio,
    vm.pb_ratio,
    vm.roe,
    vm.debt_to_equity,
    vm.net_margin
FROM Companies c
         JOIN Sectors s ON c.sector_id = s.sector_id
         JOIN ValuationMetrics vm ON c.company_id = vm.company_id
WHERE vm.calculation_date = (
    SELECT MAX(calculation_date)
    FROM ValuationMetrics
    WHERE company_id = c.company_id
);

INSERT INTO Permission (
    permission_level, level_name, level_description,
    can_create, can_read, can_update, can_delete,
    can_execute_reports, can_manage_users, can_approve
)
VALUES
    (1, 'Analyst', 'Entry-level research role', FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE),
    (3, 'Associate', 'Higher-level research role, can create content', TRUE, TRUE, TRUE, FALSE, TRUE, FALSE, TRUE),
    (10, 'Admin', 'Full system access', TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE);


INSERT INTO Role (role_name, role_description, permission_level)
VALUES
    ('Admin', 'System administrator', 10),
    ('Associate', 'Senior research role supporting lead analyst', 3),
    ('Analyst', 'Junior research role', 1);


INSERT INTO Department (department_name)
VALUES
    ('Equity Research'),
    ('Quantitative Research'),
    ('Macroeconomic Research'),
    ('Risk Management'),
    ('Compliance'),
    ('Technology & Data');


INSERT INTO Users (username, password_hash, role_id, email, department_id)
VALUES
    ('admin',
     '$2a$12$jdKmNAeDdyZ//ZtK90qyVurasMs/bIdNQJodWZoAhOyvCyRJpN1cC',
     1, 'admin@equityresearch.com', 6),

    ('analyst1',
     '$2a$12$jdKmNAeDdyZ//ZtK90qyVurasMs/bIdNQJodWZoAhOyvCyRJpN1cC',
     2, 'analyst1@equityresearch.com', 1),

    ('associate1',
     '$2a$12$jdKmNAeDdyZ//ZtK90qyVurasMs/bIdNQJodWZoAhOyvCyRJpN1cC',
     3, 'associate1@equityresearch.com', 1);


INSERT INTO Sectors (sector_id, sector_name, industry_category, sector_index_ticker)
VALUES
    (1, 'Technology', 'Software, Semiconductors, IT Services', 'XLK'),
    (2, 'Financial Services', 'Banks, Insurance, FinTech', 'XLF'),
    (3, 'Healthcare', 'Biotech, Pharmaceuticals, Medical Devices', 'XLV'),
    (4, 'Consumer Discretionary', 'Retail, Automobiles, Luxury Goods', 'XLY'),
    (5, 'Energy', 'Oil & Gas, Renewables, Utilities', 'XLE'),
    (6, 'Industrials', 'Manufacturing, Aerospace, Transportation', 'XLI'),
    (7, 'Communications Services', 'Telecom, Media, Entertainment', 'XLC'),
    (8, 'Consumer Staples', 'Food, Beverages, Household Products', 'XLP'),
    (9, 'Real Estate', 'REITs, Commercial Real Estate', 'XLRE'),
    (10, 'Materials', 'Chemicals, Metals, Mining', 'XLB'),
    (11, 'Utilities', 'Electricity, Water, Infrastructure', 'XLU');

DELIMITER //

-- Procedure 1: Authenticate User
CREATE PROCEDURE AuthenticateUser(
    IN p_username VARCHAR(50),
    IN p_password_hash VARCHAR(255)
)
BEGIN
SELECT
    u.user_id,
    u.username,
    u.email,
    u.full_name,
    u.role_id,
    u.is_active,
    r.role_name,
    r.permission_level,
    p.level_name,
    p.can_create,
    p.can_read,
    p.can_update,
    p.can_delete,
    p.can_execute_reports,
    p.can_manage_users,
    p.can_approve,
    d.department_name
FROM Users u
         INNER JOIN Role r ON u.role_id = r.role_id
         INNER JOIN Permission p ON r.permission_level = p.permission_level
         INNER JOIN Department d on u.department_id = d.department_id
WHERE u.username = p_username
  AND u.password_hash = p_password_hash
  AND u.is_active = TRUE;

UPDATE Users
SET last_login = CURRENT_TIMESTAMP,
    days_since_last_login = 0
WHERE username = p_username;
END //

CREATE PROCEDURE GetTopPerformers(
    IN p_days INT,
    IN p_limit INT
)
BEGIN
SELECT
    c.company_id,
    c.ticker_symbol,
    c.company_name,
    s.sector_name,
    MIN(sp.close_price) as start_price,
    MAX(sp.close_price) as end_price,
    ((MAX(sp.close_price) - MIN(sp.close_price)) / MIN(sp.close_price)) * 100 as return_pct
FROM StockPrices sp
         INNER JOIN Companies c ON sp.company_id = c.company_id
         INNER JOIN Sectors s ON c.sector_id = s.sector_id
WHERE sp.trade_date >= DATE_SUB(CURDATE(), INTERVAL p_days DAY)
GROUP BY c.company_id, c.ticker_symbol, c.company_name, s.sector_name
ORDER BY return_pct DESC
    LIMIT p_limit;
END //

CREATE PROCEDURE DeleteCompanyWithDependencies(IN p_company_id INT)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
ROLLBACK;
RESIGNAL;
END;

START TRANSACTION;

IF NOT EXISTS (SELECT 1 FROM Companies WHERE company_id = p_company_id) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Company not found';
END IF;

DELETE FROM Companies WHERE company_id = p_company_id;

COMMIT;
END //

DELIMITER ;

DROP FUNCTION GetMostActiveDepartment
    DELIMITER //
CREATE FUNCTION GetMostActiveDepartment()
    RETURNS VARCHAR(100)
    DETERMINISTIC
    READS SQL DATA
    COMMENT 'Get name of department with most active users'
BEGIN
    DECLARE dept_name VARCHAR(100);

SELECT d.department_name INTO dept_name
FROM Department d
         LEFT JOIN Users u ON d.department_id = u.department_id AND u.is_active = TRUE
GROUP BY d.department_id, d.department_name
ORDER BY COUNT(u.user_id) DESC
    LIMIT 1;

RETURN IFNULL(dept_name, 'No Active Departments');
END//

DELIMITER //
CREATE TRIGGER trg_before_user_update
    BEFORE UPDATE ON Users
    FOR EACH ROW
BEGIN
    IF NEW.last_login IS NOT NULL AND NEW.last_login != OLD.last_login THEN
        SET NEW.days_since_last_login = 0;
END IF;
END //

DELIMITER //

CREATE EVENT IF NOT EXISTS evt_update_user_inactive_days
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_DATE + INTERVAL 1 DAY
DO
BEGIN
    -- Update days_since_last_login for all users
UPDATE Users
SET days_since_last_login = CASE
                                WHEN last_login IS NULL THEN NULL
                                ELSE DATEDIFF(CURDATE(), DATE(last_login))
    END
WHERE is_active = TRUE;
END //
