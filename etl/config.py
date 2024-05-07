EXPENSES_FIELD_MAPPING = {
    "Fund@Code": "fund",
    "Fund@Long Name": "fund_name",
    "Department@Dept": "department",
    "Department@Long Name": "department_name",
    "Unit@Unit Code": "unit",
    "Unit@Long Name": "unit_name",
    "Object Code Category@Code": "object_category_code",
    "Object Code Category@Long Name": "object_category_name",
    "Object Code@Code": "object_code",
    "Object Code@Long Name": "object_name",
    "Object Code - Spending Plan@Code": "object_spend_plan_code",
    "Object Code - Spending Plan@Long Name": "object_spend_plan_name",
    "Budget YTD with Period Cutoff": "budget_ytd_with_period_cutoff",
    "Proposed Budget CYE Amount FY & BFY Prompted (Phase 6)": "proposed_budget_cye_amount",
    "Expenses MTD with Period Cutoff": "expenses_mtd",
    "Expenses YTD with Period Cutoff": "expenses_ytd",
    "Encumbrance YTD with Period Cutoff": "encumbrance_ytd",
    "Unobligated": "unobligated",
    "% Obligated": "percent_obligated",
}

REVENUE_FIELD_MAPPING = {
    "Fund@Code": "fund",
    "Fund@Long Name": "fund_name",
    "Department@Dept": "department",
    "Department@Long Name": "department_name",
    "Unit@Unit Code": "unit",
    "Unit@Long Name": "unit_name",
    "Revenue Source Category@Code": "revenue_source_category_code",
    "Revenue Source Category@Long Name": "revenue_source_category_name",
    "Revenue Source@Code": "revenue_source_code",
    "Revenue Source@Long Name": "revenue_source_name",
    "Revenue Budget YTD with Period Cutoff": "revenue_budget_ytd",
    "Proposed Revenue Amount CYE Prompted FY & BFY (Phase 6)": "revenue_cye",
    "Revenue MTD with Period Cutoff": "revenue_mtd",
    "Revenue YTD with Period Cutoff": "revenue_ytd",
    "Revenue ITD with Period Cutoff": "revenue_itd",
    "Unrecognized Amount": "unrecognized_amount",
    "% Unrecognized": "percent_unrecognized",
}

EXPENSES_NUMERIC_COLS = [
    "budget_ytd_with_period_cutoff",
    "proposed_budget_cye_amount",
    "expenses_mtd",
    "expenses_ytd",
    "encumbrance_ytd",
    "unobligated",
    "percent_obligated",
]

REVENUE_NUMERIC_COLS = [
    "revenue_ytd",
    "revenue_budget_ytd",
    "revenue_cye",
    "revenue_mtd",
    "revenue_itd",
    "unrecognized_amount",
    "percent_unrecognized",
]

REVENUE_ID_COLUMN = [
    "fiscal_year",
    "fiscal_month",
    "fund",
    "department",
    "unit",
    "revenue_source_category_code",
    "revenue_source_code",
]

EXPENSES_ID_COLUMN = [
    "fiscal_year",
    "fiscal_month",
    "fund",
    "department",
    "unit",
    "object_code",
]