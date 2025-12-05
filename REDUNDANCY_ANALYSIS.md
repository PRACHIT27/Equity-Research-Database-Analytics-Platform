# Code Redundancy Analysis: etl_pipeline2.py

## Summary
Identified **7 major categories of redundancy** causing ~400+ lines of duplicate/unnecessary code.

---

## 1. DUPLICATED BALANCE SHEET LOADING METHOD (CRITICAL)
**Lines:** 564-890 + 710-840  
**Severity:** ⚠️ HIGH - Entire duplicate method exists  
**Issue:** Complete duplicate of `load_balance_sheet()` method appears INSIDE `load_cashflow_statement()` method

### Problem:
```python
# Line 564-710: load_cashflow_statement() method
# Line 710-840: Duplicate load_balance_sheet() code appears here
```

The `load_cashflow_statement()` method has a complete copy of `load_balance_sheet()` code at the end (lines 710-840). This is a broken/incomplete refactoring.

**Impact:** 
- Duplicate code block (~130 lines)
- Confusing method structure
- Maintenance nightmare
- Makes the class impossible to understand

---

## 2. REPEATED FINANCIAL STATEMENT ID RETRIEVAL PATTERN
**Lines:** 281-310, 427-456, 564-600  
**Severity:** 🔴 MEDIUM - Appears 3 times in load_income_statement, load_balance_sheet, load_cashflow_statement  
**Code Pattern:**
```python
# Pattern repeated in all 3 methods:
fiscal_year = period_date.year
fiscal_quarter = f"Q{(period_date.month - 1) // 3 + 1}"

query = """
INSERT INTO FinancialStatements 
(company_id, fiscal_year, fiscal_quarter, filing_date, statement_type)
VALUES (%s, %s, %s, %s, '<STATEMENT_TYPE>')
ON DUPLICATE KEY UPDATE statement_id=LAST_INSERT_ID(statement_id)
"""

statement_id = self.execute_query(query, (
    company_id, fiscal_year, fiscal_quarter, period_date
))

if not statement_id:
    query = """
    SELECT statement_id FROM FinancialStatements 
    WHERE company_id=%s AND fiscal_year=%s AND fiscal_quarter=%s 
    AND statement_type='<STATEMENT_TYPE>'
    """
    result = self.execute_query(query, (company_id, fiscal_year, fiscal_quarter), fetch=True)
    if result:
        statement_id = result[0]['statement_id']
```

**Solution:** Extract to helper method:
```python
def _get_or_create_statement(self, company_id, period_date, statement_type):
    """Get or create financial statement record"""
    fiscal_year = period_date.year
    fiscal_quarter = f"Q{(period_date.month - 1) // 3 + 1}"
    
    query = """
    INSERT INTO FinancialStatements 
    (company_id, fiscal_year, fiscal_quarter, filing_date, statement_type)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE statement_id=LAST_INSERT_ID(statement_id)
    """
    
    statement_id = self.execute_query(query, (
        company_id, fiscal_year, fiscal_quarter, period_date, statement_type
    ))
    
    if not statement_id:
        query = """
        SELECT statement_id FROM FinancialStatements 
        WHERE company_id=%s AND fiscal_year=%s AND fiscal_quarter=%s 
        AND statement_type=%s
        """
        result = self.execute_query(query, (company_id, fiscal_year, fiscal_quarter, statement_type), fetch=True)
        if result:
            statement_id = result[0]['statement_id']
    
    return statement_id
```

**Impact:** Removes ~80 lines of duplicate code

---

## 3. REPEATED FIELD EXTRACTION PATTERNS
**Severity:** 🟡 MEDIUM - Pattern repeated 40+ times across load_* methods  
**Issue:** Multiple variations of the same pattern:
```python
# Pattern repeated for nearly every field:
field = self.safe_get(statement_data, 'Option1') or \
        self.safe_get(statement_data, 'Option2') or \
        self.safe_get(statement_data, 'Option3')
```

This appears in:
- `load_income_statement()`: ~15 fields
- `load_balance_sheet()`: ~15 fields  
- `load_cashflow_statement()`: ~8 fields

**Solution:** Create a field mapping dictionary:
```python
FIELD_MAPPINGS = {
    'revenue': ['Total Revenue', 'Revenue'],
    'cost_of_revenue': ['Cost Of Revenue', 'Total Cost Of Revenue'],
    'gross_profit': ['Gross Profit'],
    'operating_expenses': ['Operating Expense', 'Total Operating Expenses', 'Operating Expenses'],
    # ... etc
}

def _extract_fields(self, statement_data, field_names_map):
    """Extract multiple fields using fallback options"""
    result = {}
    for field_key, possible_names in field_names_map.items():
        for name in possible_names:
            value = self.safe_get(statement_data, name)
            if value is not None:
                result[field_key] = value
                break
        if field_key not in result:
            result[field_key] = None
    return result
```

**Impact:** Removes ~60 lines, improves maintainability

---

## 4. REPEATED NULL/ZERO VALIDATION LOGIC
**Lines:** Throughout load_income_statement, load_balance_sheet, load_cashflow_statement  
**Severity:** 🟡 MEDIUM - Repeated 6+ times  
**Pattern:**
```python
# Appears multiple times in different forms:
if net_income is not None and basic_shares is not None and basic_shares > 0:
    eps_basic = net_income / basic_shares
else:
    eps_basic = self.safe_get(statement_data, 'Basic EPS')

# Similar pattern for capital_expenditure, etc.
```

**Solution:** Create helper method:
```python
def _safe_divide(self, numerator, denominator, fallback_key=None, statement_data=None):
    """Safely divide with fallback option"""
    if numerator is not None and denominator is not None and denominator > 0:
        return numerator / denominator
    elif fallback_key and statement_data is not None:
        return self.safe_get(statement_data, fallback_key)
    return None
```

**Impact:** Removes ~20 lines of validation logic

---

## 5. REPEATED INSERT/UPDATE QUERY PATTERN (3 variations)
**Lines:** 360-390, 500-530, 655-685  
**Severity:** 🟡 MEDIUM - Pattern appears 3 times  
**Issue:** Each statement type has nearly identical INSERT...ON DUPLICATE KEY UPDATE pattern

All three have same structure:
```python
query = """
INSERT INTO <TABLE_NAME>
(<columns>)
VALUES (<placeholders>)
ON DUPLICATE KEY UPDATE
<all_columns> = VALUES(<columns>)
"""

params = (all_params)
self.execute_query(query, params)
return True
```

**Solution:** Create a generic loader:
```python
def _load_statement_details(self, table_name, statement_id, fields_dict):
    """Generic method to load statement details"""
    columns = ', '.join(fields_dict.keys())
    placeholders = ', '.join(['%s'] * len(fields_dict))
    updates = ', '.join([f"{k} = VALUES({k})" for k in fields_dict.keys()])
    
    query = f"""
    INSERT INTO {table_name}
    (statement_id, {columns})
    VALUES (%s, {placeholders})
    ON DUPLICATE KEY UPDATE
    {updates}
    """
    
    params = (statement_id, *fields_dict.values())
    return self.execute_query(query, params) is not None
```

**Impact:** Removes ~40 lines of SQL boilerplate

---

## 6. REPEATED FORECAST INTEGRATION CODE
**Lines:** 1289-1319, 1360-1390  
**Severity:** 🟡 MEDIUM - Same pattern in two methods  
**Issue:** `_generate_current_forecast()` and `_generate_periodic_forecasts()` both do:

```python
# In _generate_current_forecast():
stock_forecast = self.calculate_stock_price_forecast(price_data, company_id=company_id)
price_forecast_data = {...}
revenue_forecast_data = {...}
eps_forecast_data = {...}
self.load_forecast(...)

# In _generate_periodic_forecasts():
stock_forecast = self.calculate_stock_price_forecast(price_data, company_id=company_id, ...)
price_forecast_data = {...}
revenue_forecast_data = {...}
eps_forecast_data = {...}
self.load_forecast(...)
```

**Solution:** Extract to helper:
```python
def _process_and_load_forecast(self, company_id, stock_forecast, forecast_date=None, target_date=None):
    """Process forecast calculation and load to database"""
    if not stock_forecast:
        return False
    
    price_forecast_data = {
        'target_price': stock_forecast.get('target_price'),
        'recommendation': stock_forecast.get('recommendation'),
        'confidence_score': stock_forecast.get('confidence_score')
    }
    
    revenue_forecast_data = {
        'forecasted_annual_revenue': stock_forecast.get('revenue_forecasted')
    }
    
    eps_forecast_data = {
        'forecasted_eps': stock_forecast.get('eps_forecasted')
    }
    
    return self.load_forecast(company_id, price_forecast_data, revenue_forecast_data, 
                            eps_forecast_data, forecast_date, target_date)
```

**Impact:** Removes ~40 lines of duplicate forecast handling

---

## 7. REPEATED ERROR HANDLING PATTERN
**Severity:** 🟡 LOW - Minor but appears 5+ times  
**Pattern:**
```python
except Exception as e:
    print(f"✗ Error [description]: {e}")
    return False
```

Could be standardized with logging, but less critical than others.

---

## BONUS ISSUE: INCOMPLETE METHOD AT END OF FILE
**Lines:** 710-840 (inside load_cashflow_statement)  
**Severity:** 🔴 CRITICAL  

The `load_cashflow_statement()` method has a comment inside it suggesting another method definition:
```python
        except Exception as e:
            print(f"    ✗ Error loading cash flow statement: {e}")
            import traceback
            traceback.print_exc()
            return False
        """
        Load balance sheet data with comprehensive field mapping   <-- WRONG!
        
        Args:
            company_id: Company ID in database
            statement_data: Series containing balance sheet data
            period_date: Date of the financial statement
            
        Returns:
            Boolean indicating success
        """
        fiscal_year = period_date.year
        ...
```

This is a BROKEN METHOD DEFINITION. The docstring is in a comment, and the method code is in the wrong place.

---

## SUMMARY TABLE

| Issue | Type | Lines | Severity | Solution | Savings |
|-------|------|-------|----------|----------|---------|
| Duplicate load_balance_sheet in load_cashflow_statement | Structural | 710-840 | 🔴 CRITICAL | Remove duplicate | 130 lines |
| Repeated statement ID retrieval | Pattern | 281-310, 427-456, 564-600 | 🔴 MEDIUM | Extract to helper | 80 lines |
| Field extraction repetition | Pattern | 40+ occurrences | 🟡 MEDIUM | Use field mapping dict | 60 lines |
| NULL/zero validation logic | Pattern | 6+ occurrences | 🟡 MEDIUM | Extract helper method | 20 lines |
| INSERT/UPDATE query boilerplate | SQL | 3 locations | 🟡 MEDIUM | Create generic loader | 40 lines |
| Forecast integration code | Logic | 2 locations | 🟡 MEDIUM | Extract helper | 40 lines |
| Error handling patterns | Pattern | 5+ locations | 🟡 LOW | Standardize logging | 10 lines |

**Total Potential Reduction:** ~380-400 lines of code

---

## Recommended Refactoring Priority

1. **FIRST:** Fix the structural issue (remove duplicate load_balance_sheet code - 130 lines removed instantly)
2. **SECOND:** Extract `_get_or_create_statement()` helper (reduces 3 methods)
3. **THIRD:** Create field mapping system for financial statements
4. **FOURTH:** Create generic `_load_statement_details()` method
5. **FIFTH:** Extract forecast processing helper
6. **SIXTH:** (Optional) Standardize error handling with logging

This refactoring would improve code maintainability, reduce testing burden, and make the codebase much cleaner.
