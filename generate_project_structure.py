"""
Project Structure Generator
Run this script to automatically create all necessary folders and __init__.py files
"""

import os

def create_project_structure():
    """Create complete project directory structure"""

    # Define project structure
    structure = {
        'config': ['__init__.py', 'database.py'],
        'core': ['__init__.py', 'DatabaseConnection.py'],
        'repositories': [
            '__init__.py',
            'BaseRepository.py',
            'CompanyRepository.py',
            'FinancialRepository.py',
            'PriceRepository.py',
            'ForecastRepository.py',
            'UserRepository.py',
            'valuation_repository.py',
            'watchlist_repository.py'
        ],
        'services': [
            '__init__.py',
            'CompanyService.py',
            'FinancialService.py',
            'PriceService.py',
            'ForecastService.py',
            'UserService.py',
            'ValuationService.py',
            'AnalyticsService.py'
        ],
        'models': [
            '__init__.py',
            'company.py',
            'financial_statement.py',
            'stock_price.py',
            'forecast.py',
            'user.py'
        ],
        'utils': [
            '__init__.py',
            'validators.py',
            'formatters.py',
            'calculators.py',
            'exceptions.py'
        ],
        'ui': ['__init__.py'],
        'ui/pages': [
            '__init__.py',
            'dashboard.py',
            'companies.py',
            'financial_statements.py',
            'stock_prices.py',
            'forecasts.py',
            'valuation_metrics.py',
            'users.py',
            'analytics.py'
        ],
        'ui/components': [
            '__init__.py',
            'sidebar.py',
            'tables.py',
            'charts.py'
        ]
    }

    # Create directories and files
    for directory, files in structure.items():
        # Create directory
        os.makedirs(directory, exist_ok=True)
        print(f"✓ Created directory: {directory}/")

        # Create files
        for file in files:
            filepath = os.path.join(directory, file)
            if not os.path.exists(filepath):
                with open(filepath, 'w') as f:
                    if file == '__init__.py':
                        f.write('"""Package initialization"""\\n')
                    else:
                        f.write(f'"""\\n{file.replace(".py", "").replace("_", " ").title()} Module\\nTODO: Implement functionality\\n"""\\n\\n')
                print(f"  ✓ Created file: {filepath}")

    # Create root-level files
    root_files = {
        'requirements.txt': """# Database
pymysql==1.1.0

# Web Framework
streamlit==1.29.0

# Data Processing
pandas==2.1.4
numpy==1.26.2

# Visualization
plotly==5.18.0

# Security
bcrypt==4.1.2

# Utilities
python-dateutil==2.8.2
""",
        '.gitignore': """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Database
*.sql.backup
*.db

# OS
.DS_Store
Thumbs.db

# Streamlit
.streamlit/secrets.toml
""",
        'app.py': """\"\"\"
Main Streamlit Application Entry Point
Run with: streamlit run app.py
\"\"\"

import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Equity Research Platform",
    page_icon="📊",
    layout="wide"
)

st.title("🏦 Equity Research Database & Analytics Platform")
st.markdown("---")

st.markdown(\"\"\"
### Welcome!

This is your Equity Research Platform.

**Next Steps:**
1. Configure database connection in `config/database.py`
2. Import schema: `mysql < schema_dump.sql`
3. Implement repository classes
4. Implement service classes
5. Build UI pages

**Login Credentials (Demo):**
- Admin: `admin` / `password`
- Analyst: `analyst1` / `password`
\"\"\"
)

# TODO: Implement full application logic
# Import and initialize components here
"""
    }

    for filename, content in root_files.items():
        if not os.path.exists(filename):
            with open(filename, 'w') as f:
                f.write(content)
            print(f"✓ Created file: {filename}")

    print("\\n" + "="*60)
    print("✅ Project structure created successfully!")
    print("="*60)
    print("\\nNext steps:")
    print("1. Review the README.md for detailed setup instructions")
    print("2. Configure database credentials in config/database.py")
    print("3. Import schema: mysql < schema_dump.sql")
    print("4. Install dependencies: pip install -r requirements.txt")
    print("5. Implement the provided code modules")
    print("6. Run application: streamlit run app.py")

if __name__ == "__main__":
    create_project_structure()