# Equity Research Database & Analytics Platform

**Group:** TupePSharmaS  
**Members:** Prachit Tupe, Siddhant Sharma  
**Course:** Database Management Systems

## 📋 Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Setup Instructions](#setup-instructions)
- [Project Structure](#project-structure)
- [Features](#features)
- [Usage Guide](#usage-guide)
- [Technical Specifications](#technical-specifications)
- [Database Schema](#database-schema)
- [Design Patterns](#design-patterns)

---

## 🎯 Project Overview

The **Equity Research Database & Analytics Platform** is a comprehensive financial data management system designed for managing and analyzing publicly traded company information. The system provides role-based access to financial statements, stock prices, forecasts, and valuation metrics through a modern web interface.

### Key Objectives
- Efficient storage and retrieval of multi-year financial data
- Complex analytical queries for investment decision-making
- Role-based access control (Admin, Analyst, Associate)
- Real-time data visualization and reporting
- Professional-grade software architecture

---

## 🏗️ Architecture

This project implements **enterprise-level software architecture** with clear separation of concerns:

```
├── Presentation Layer (UI)          → Streamlit web interface
├── Service Layer (Business Logic)   → Validation, orchestration
├── Repository Layer (Data Access)   → SQL queries, CRUD operations
├── Core Layer (Infrastructure)      → Database connection management
└── Configuration Layer              → Database settings
```

### Design Patterns Used
1. **Repository Pattern** - Abstracts data access logic
2. **Service Layer Pattern** - Encapsulates business logic
3. **Singleton Pattern** - Database connection management
4. **Data Transfer Objects (DTOs)** - Type-safe data models
5. **Dependency Injection** - Loose coupling between layers

---

## 🚀 Setup Instructions

### Prerequisites
- **Python 3.11+**
- **MySQL 8.0+**
- **pip** (Python package manager)
- **Git** (for cloning repository)

### Step 1: Install MySQL
```bash
# macOS (using Homebrew)
brew install mysql
brew services start mysql

# Ubuntu/Debian
sudo apt-get update
sudo apt-get install mysql-server
sudo systemctl start mysql

# Windows
# Download from: https://dev.mysql.com/downloads/installer/
```

### Step 2: Clone Repository
```bash
git clone <repository-url>
cd equity-research-db
```

### Step 3: Create Database
```bash
# Login to MySQL
mysql -u root -p

# Create database and import schema
SOURCE schema_dump.sql;

# Verify tables created
SHOW TABLES;
EXIT;
```

### Step 4: Configure Database Connection
Edit `config/database.py`:
```python
HOST = 'localhost'
PORT = 3306
USER = 'root'          # Your MySQL username
PASSWORD = ''          # Your MySQL password
DATABASE = 'equity_research'
```
Edit `utils/etl.py`:
```python
db_config = {
        'host': 'localhost',
        'user': '', # Your username
        'password' : '', # your password
        'database': 'equity_research',
    }
```

### Step 5: Install Python Dependencies
```bash
# Create virtual environment (recommended)
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 6: Run Application
```bash
python utils/etl.py

streamlit run app.py
```

The application will open in your browser at `http://localhost:8501`

---

## 📁 Project Structure

```
equity-research-db/
│
├── app.py                          # Main Streamlit application entry point
├── requirements.txt                # Python dependencies
├── README.md                       # Project documentation
├── schema_dump.sql                 # Complete database schema with sample data
│
├── config/                         # Configuration Layer
│   ├── __init__.py
│   └── database.py                 # Database configuration settings
│
├── core/                           # Core Infrastructure Layer
│   ├── __init__.py
│   └── connection.py               # Database connection manager (Singleton)
│
├── repositories/                   # Data Access Layer (Repository Pattern)
│   ├── __init__.py
│   ├── base_repository.py          # Abstract base repository
│   ├── company_repository.py       # Company & Sector data access
│   ├── financial_repository.py     # Financial statements data access
│   ├── price_repository.py         # Stock prices data access
│   ├── forecast_repository.py      # Forecasts data access
│   ├── user_repository.py          # User management data access
│   ├── valuation_repository.py     # Valuation metrics data access
│   └── watchlist_repository.py     # Watchlist data access
│
├── services/                       # Business Logic Layer (Service Pattern)
│   ├── __init__.py
│   ├── company_service.py          # Company business logic & validation
│   ├── financial_service.py        # Financial analysis logic
│   ├── price_service.py            # Price calculations & analytics
│   ├── forecast_service.py         # Forecast management logic
│   ├── user_service.py             # Authentication & authorization
│   ├── valuation_service.py        # Valuation calculations
│   └── analytics_service.py        # Analytics & reporting logic
│
├── models/                         # Domain Models (DTOs)
│   ├── __init__.py
│   ├── company.py                  # Company domain model
│   ├── financial_statement.py      # Financial statement model
│   ├── stock_price.py              # Stock price model
│   ├── forecast.py                 # Forecast model
│   └── user.py                     # User model
│
├── utils/                          # Utility Functions
│   ├── __init__.py
│   ├── validators.py               # Input validation utilities
│   ├── formatters.py               # Display formatting utilities
│   ├── calculators.py              # Financial calculations
│   └── exceptions.py               # Custom exception classes
│
└── ui/                             # Presentation Layer (Streamlit)
    ├── __init__.py
    ├── pages/                      # Individual page components
    │   ├── __init__.py
    │   ├── dashboard.py            # Dashboard page
    │   ├── companies.py            # Companies management
    │   ├── financial_statements.py # Financial statements
    │   ├── stock_prices.py         # Stock prices
    │   ├── forecasts.py            # Forecasts
    │   ├── valuation_metrics.py    # Valuation metrics
    │   ├── users.py                # User management
    │   ├── watchlist.py            # Personal watchlist
    │   └── analytics.py            # Analytics & reports
    └── components/                 # Reusable UI components
        ├── __init__.py
        ├── sidebar.py              # Sidebar navigation
        ├── tables.py               # Table components
        └── charts.py               # Chart components
```

---

## ✨ Features

### Core Features (Required)
✅ **Complete CRUD Operations** on all entities  
✅ **9 Normalized Tables** (3NF) with proper relationships  
✅ **Primary & Foreign Keys** with ON UPDATE/ON DELETE clauses  
✅ **Stored Procedures** (GetCompanyOverview, InsertStockPrice, etc.)  
✅ **User-Defined Functions** (CalculateDailyReturn, GetLatestPrice)  
✅ **Triggers** (Validation, Logging, Auto-updates)  
✅ **Field Constraints** (NOT NULL, UNIQUE, CHECK, DEFAULT)  
✅ **Indexes** for performance optimization  
✅ **Error Handling** with try-catch mechanisms  
✅ **Role-Based Access Control** (Admin, Analyst, Associate)  


---

## 📖 Usage Guide

### Login Credentials (Demo)
```
Admin Account:
  Username: admin
  Password: password

Analyst Account:
  Username: analyst1
  Password: password

Associate Account:
  Username: associate1
  Password: password
```

### Main Features

#### 1. **Dashboard**
- Overview of database statistics
- Top performing stocks
- Sector distribution charts
- Latest recommendations

#### 2. **Company Management** (CRUD)
- **Create:** Add new companies with validation
- **Read:** View all companies, search by ticker
- **Update:** Modify company information
- **Delete:** Remove companies with dependency handling

#### 3. **Financial Statements**
- Add income statements, balance sheets, cash flow statements
- View multi-period financial data
- Analyze financial trends

#### 4. **Stock Prices**
- Add daily stock prices
- View price history with candlestick charts
- Calculate moving averages and returns
- Price statistics and analytics

#### 5. **Forecasts**
- Create price targets and recommendations
- View analyst forecasts
- Track forecast accuracy

#### 6. **Valuation Metrics**
- P/E ratio, P/B ratio, ROE analysis
- Sector-wise valuation comparison
- Custom metric calculations

#### 7. **Watchlist**
- Personal stock watchlist
- Price alerts
- Custom notes

#### 8. **Analytics & Reports**
- Sector analysis
- Performance rankings
- Custom SQL queries (Admin only)

---

## 🔧 Technical Specifications

### Technology Stack
| Component | Technology |
|-----------|------------|
| **Database** | MySQL 8.0+ |
| **Backend Language** | Python 3.11+ |
| **Database Driver** | pymysql 1.1.0 (NO ORM) |
| **Web Framework** | Streamlit 1.29.0 |
| **Data Processing** | Pandas 2.1.4 |
| **Visualization** | Plotly 5.18.0 |
| **Security** | bcrypt 4.1.2 |


### System Requirements
- **RAM:** Minimum 4GB (8GB recommended)
- **Storage:** 10GB available
- **OS:** Windows 10+, macOS 10.14+, Linux (Ubuntu 20.04+)
- **Browser:** Chrome, Firefox, Safari, Edge (latest versions)

---

## 🗄️ Database Schema

### Tables (9 Total - 2-Person Group Requirement)

1. **Sectors** - Industry sectors
2. **Companies** - Company master data
3. **Users** - System users with roles
4. **FinancialStatements** - Income statements, balance sheets, cash flow
5. **StockPrices** - Daily stock price data
6. **ValuationMetrics** - Calculated financial ratios
7. **Forecasts** - Analyst predictions and recommendations
8. **UserActivityLog** - Audit trail
9. **Watchlist** - User watchlists
    

## 📊 Stored Procedures & Functions

### Stored Procedures
1. **GetCompanyOverview** - Comprehensive company data
2. **InsertStockPrice** - Price insertion with validation
3. **CalculateValuationMetrics** - Auto-calculate financial ratios
4. **DeleteCompanyWithDependencies** - Safe deletion

### User-Defined Functions
1. **CalculateDailyReturn** - Daily return percentage
2. **GetLatestPrice** - Most recent stock price
3. **CalculateAverageVolume** - Average trading volume

### Triggers
1. **trg_after_company_insert** - Log company creation
2. **trg_before_stock_price_insert** - Validate price data
3. **trg_update_last_login** - Update user last login
4. **trg_before_forecast_insert** - Validate forecast dates

---

## 🧪 Testing the Application

### Verify Database Setup
```sql
-- Check tables
SHOW TABLES;

-- Check sample data
SELECT COUNT(*) FROM Companies;
SELECT COUNT(*) FROM StockPrices;

-- Test stored procedure
CALL GetCompanyOverview('AAPL');

-- Test function
SELECT CalculateDailyReturn('AAPL', '2024-11-05');
```

### Test Application Features
1. **Login** with demo credentials
2. **Create** a new company
3. **View** company in MySQL Workbench
4. **Update** company details
5. **Verify** changes in database
6. **Delete** company
7. **Confirm** deletion in database

---
## 🤝 Contributors

**Group:** TupePSharmaS

- **Prachit Tupe** - Database design, Backend development
- **Siddhant Sharma** - Frontend development, Testing



## 🎓 Lessons Learned

1. **Enterprise Architecture** - Implementing Repository and Service patterns significantly improved code organization and testability
2. **Direct SQL vs ORM** - Using pymysql driver provided better understanding of SQL operations and query optimization
3. **Database Design** - Proper normalization and indexing crucial for performance with large datasets
4. **Error Handling** - Comprehensive exception handling at each layer prevents cascading failures
5. **UI/UX** - Streamlit enables rapid prototyping of professional-looking interfaces

## 🚀 Future Work

1. **Real-time Data Integration** - Connect to live market data APIs
2. **Machine Learning** - Predictive models for stock price forecasting
3. **Advanced Analytics** - Portfolio optimization algorithms
4. **Multi-user Collaboration** - Shared workspaces and reports
5. **Data Export** - PDF reports and Excel exports
6. **Notification System** - Email alerts for price targets

---

**End of README**
