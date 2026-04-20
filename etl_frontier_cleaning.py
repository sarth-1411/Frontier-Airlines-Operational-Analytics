"""
Frontier Airlines - Data Cleaning & ETL Pipeline
=================================================
Author  : [Your Name]
Role    : Senior Analyst Candidate - Scheduling Analytics
Purpose : Transforms 4 raw BTS Form 41 .xls files into a clean
          star-schema data model ready for Power BI / SQL / Python analytics.

Input files (BTS Form 41 via DOT):
    - Frontier_Airlines_Employee_Data_and_Analysis.xls
    - Frontier_Airlines_Aircraft_Operating_Statistics-_Actuals.xls
    - Traffic_Capacity_Frontier_Airlines.xls
    - Frontier_Airlines_Aircraft_Operating_Statistics-_Cost_Per_Block_Hour__Unadjusted_.xls

Output CSVs (star schema):
    FACTS
    ├── fact_annual_operations.csv      — 26 rows × 41 cols, one row per year
    ├── fact_fleet_by_type.csv          — fleet-type × year (Small NB, Large NB, Widebody)
    ├── fact_geography.csv              — region × year (Domestic, Int'l, Latin Am, etc.)
    └── fact_employee_productivity.csv  — employee group × year

    DIMENSIONS
    ├── dim_date.csv                    — year, era, decade, flags
    ├── dim_fleet_type.csv              — fleet type metadata
    ├── dim_region.csv                  — geographic region metadata
    └── dim_employee_group.csv          — employee group metadata

Data Challenges Handled:
    - Pivot-style .xls layout (years as columns, metrics as rows) → normalized rows
    - Dual header structure: col A = section label, col B = metric label
    - Years in Employee file start col 1 (not col 2 like other files)
    - Zero values in early years (1995-1997) where Frontier hadn't started BTS reporting
    - Derived KPIs computed: ancillary revenue, RASM-CASM spread, fuel efficiency
    - Era classification for strategic segmentation

Usage:
    pip install pandas xlrd openpyxl
    python etl_frontier_cleaning.py

    Optional - specify custom paths:
    python etl_frontier_cleaning.py --data_dir ./raw_data --out_dir ./clean_data
"""

import argparse
import os
import pandas as pd
import numpy as np

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

YEARS = list(range(1995, 2021))  # 26 years of BTS data

RAW_FILES = {
    "employee":    "Frontier_Airlines_Employee_Data_and_Analysis.xls",
    "actuals":     "Frontier_Airlines_Aircraft_Operating_Statistics-_Actuals.xls",
    "traffic":     "Traffic_Capacity_Frontier_Airlines.xls",
    "cost_pbh":    "Frontier_Airlines_Aircraft_Operating_Statistics-_Cost_Per_Block_Hour__Unadjusted_.xls",
}

# Strategic eras for segmentation in Power BI slicers
ERA_MAP = {
    range(1995, 2004): "Startup & Growth",
    range(2004, 2010): "Expansion & Crisis",
    range(2010, 2015): "Restructuring",
    range(2015, 2020): "ULCC Pivot",
    range(2020, 2021): "COVID Impact",
}

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def get_era(year: int) -> str:
    for yr_range, label in ERA_MAP.items():
        if year in yr_range:
            return label
    return "Unknown"


def extract_emp_row(df: pd.DataFrame, row_idx: int) -> dict:
    """
    Employee file layout: col 0 = label, cols 1..26 = years 1995..2020
    (one column offset compared to other files)
    """
    row = df.iloc[row_idx, 1:27].values
    return {
        y: (float(v) if pd.notna(v) and v != "" else None)
        for y, v in zip(YEARS, row)
    }


def extract_row(df: pd.DataFrame, row_idx: int) -> dict:
    """
    Standard file layout: col 0 = section, col 1 = metric, cols 2..27 = years 1995..2020
    """
    row = df.iloc[row_idx, 2:28].values
    return {
        y: (float(v) if pd.notna(v) and v != "" else None)
        for y, v in zip(YEARS, row)
    }


def safe_div(a, b):
    """Division safe against None and zero."""
    if a is None or b is None or b == 0:
        return None
    return a / b


# ─────────────────────────────────────────────────────────────
# LOAD RAW FILES
# ─────────────────────────────────────────────────────────────

def load_raw_files(data_dir: str) -> dict:
    dfs = {}
    for key, fname in RAW_FILES.items():
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing: {path}")
        dfs[key] = pd.read_excel(path, sheet_name=0, header=None)
        print(f"  Loaded {key}: {dfs[key].shape}")
    return dfs


# ─────────────────────────────────────────────────────────────
# TABLE 1: fact_annual_operations
# Core KPI table — one row per year, all top-level metrics
# ─────────────────────────────────────────────────────────────

def build_fact_annual(dfs: dict) -> pd.DataFrame:
    df_e  = dfs["employee"]
    df_a  = dfs["actuals"]
    df_t  = dfs["traffic"]
    df_c  = dfs["cost_pbh"]

    rows = []
    for y in YEARS:
        r = {"Year": y}

        # ── Traffic & Revenue ──────────────────────────────
        r["ASM_Total"]           = extract_row(df_t, 35)[y]
        r["RPM_Total"]           = extract_row(df_t, 34)[y]
        r["Pax_Revenue_000"]     = extract_row(df_t, 32)[y]
        r["Total_Revenue_000"]   = extract_row(df_t, 33)[y]
        r["ASM_Domestic"]        = extract_row(df_t, 23)[y]
        r["ASM_International"]   = extract_row(df_t, 29)[y]
        r["RPM_Domestic"]        = extract_row(df_t, 22)[y]
        r["RPM_International"]   = extract_row(df_t, 28)[y]
        r["PaxRev_Domestic_000"] = extract_row(df_t, 20)[y]
        r["PaxRev_Intl_000"]     = extract_row(df_t, 26)[y]

        # ── Unit Economics ─────────────────────────────────
        # Source: Traffic file rows 91, 98, 105, 112
        r["Yield_cents_per_RPM"] = extract_row(df_t, 91)[y]   # Pax Rev / RPM
        r["RASM_cents"]          = extract_row(df_t, 98)[y]   # Pax Rev / ASM
        r["CASM_cents"]          = extract_row(df_t, 105)[y]  # AC Op Expense / ASM
        r["LoadFactor_Pct"]      = extract_row(df_t, 112)[y]  # RPM / ASM
        r["LoadFactor_Dom_Pct"]  = extract_row(df_t, 110)[y]
        r["LoadFactor_Intl_Pct"] = extract_row(df_t, 111)[y]

        # ── Operations (Total Fleet, Actuals) ──────────────
        # Source: Actuals file — Total Fleet section rows 288-295
        r["BlockHours_Total"]     = extract_row(df_a, 288)[y]
        r["AirborneHours_Total"]  = extract_row(df_a, 289)[y]
        r["Departures_Total"]     = extract_row(df_a, 290)[y]
        r["Gallons_Total"]        = extract_row(df_a, 294)[y]
        r["Aircraft_InFleet"]     = extract_row(df_a, 174)[y]  # Large NB fleet proxy for total
        r["DailyUtil_BlockHrs"]   = extract_row(df_a, 181)[y]
        r["GallonsPerBlockHour"]  = extract_row(df_a, 177)[y]

        # ── Operating Expense Breakdown (000) ──────────────
        # Source: Actuals file — Total Fleet section rows 297-315
        r["OpExp_Pilots_000"]      = extract_row(df_a, 297)[y]
        r["OpExp_Fuel_000"]        = extract_row(df_a, 303)[y]
        r["OpExp_Maintenance_000"] = extract_row(df_a, 306)[y]
        r["OpExp_AircraftOwn_000"] = extract_row(df_a, 312)[y]

        # ── Workforce ──────────────────────────────────────
        # Source: Employee file
        r["Employees_Total"]        = extract_emp_row(df_e, 102)[y]
        r["Employees_Pilots"]       = extract_emp_row(df_e, 88)[y]
        r["Employees_Maintenance"]  = extract_emp_row(df_e, 91)[y]
        r["AvgSalary_Total"]        = extract_emp_row(df_e, 69)[y]
        r["AvgSalary_Pilots"]       = extract_emp_row(df_e, 64)[y]
        r["AvgSalary_Mgmt"]         = extract_emp_row(df_e, 63)[y]
        r["ASMs_per_Employee_000"]  = extract_emp_row(df_e, 32)[y]
        r["BlockHrs_per_Emp_Month"] = extract_emp_row(df_e, 24)[y]
        r["Pax_per_Employee"]       = extract_emp_row(df_e, 48)[y]

        # ── Operational Indices ────────────────────────────
        r["FuelPrice_per_Gal"]    = extract_emp_row(df_e, 7)[y]
        r["StageLength_Miles"]    = extract_emp_row(df_e, 5)[y]
        r["SeatsPerDeparture"]    = extract_emp_row(df_e, 6)[y]
        r["AirborneHrs_BlockHrs"] = extract_emp_row(df_e, 8)[y]
        r["Maint_Outsourced_Pct"] = extract_emp_row(df_e, 12)[y]

        # ── Cost Per Block Hour ────────────────────────────
        # Source: Cost Per Block Hour file — Total Fleet rows 120-157
        r["CPBH_Total"]       = extract_row(df_c, 120)[y]
        r["CPBH_ExFuel"]      = extract_row(df_c, 121)[y]
        r["CPBH_Fuel"]        = extract_row(df_c, 130)[y]
        r["CPBH_Pilots"]      = extract_row(df_c, 123)[y]
        r["CPBH_Maintenance"] = extract_row(df_c, 134)[y]
        r["CPBH_AircraftOwn"] = extract_row(df_c, 141)[y]

        # ── DERIVED KPIs ───────────────────────────────────
        r["Ancillary_Revenue_000"]   = safe_div(
            (r["Total_Revenue_000"] or 0) - (r["Pax_Revenue_000"] or 0), 1
        )
        r["RASM_CASM_Spread"]        = (
            (r["RASM_cents"] or 0) - (r["CASM_cents"] or 0)
        ) or None
        r["Intl_ASM_Share_Pct"]      = safe_div(
            r["ASM_International"], r["ASM_Total"]
        )
        r["Revenue_per_Emp_000"]     = safe_div(
            r["Total_Revenue_000"], r["Employees_Total"]
        )
        r["FuelCost_per_ASM_cents"]  = safe_div(
            (r["OpExp_Fuel_000"] or 0) * 100000, r["ASM_Total"]
        )
        r["Ancillary_Pct_of_Rev"]    = safe_div(
            r["Ancillary_Revenue_000"], r["Total_Revenue_000"]
        )
        r["FuelBurn_ASM_per_Gal"]    = safe_div(
            r["ASM_Total"], r["Gallons_Total"]
        )

        # ── Flags & Labels ────────────────────────────────
        r["Era"]          = get_era(y)
        r["Era_Order"]    = [1,2,3,4,5][[
            "Startup & Growth","Expansion & Crisis","Restructuring",
            "ULCC Pivot","COVID Impact"
        ].index(r["Era"])]
        r["Is_ULCC_Era"]  = 1 if y >= 2015 else 0
        r["Is_Full_Year"] = 0 if y == 2020 else 1

        rows.append(r)

    df = pd.DataFrame(rows)
    print(f"  fact_annual_operations: {df.shape}")
    return df


# ─────────────────────────────────────────────────────────────
# TABLE 2: fact_fleet_by_type
# Per fleet type (Small NB, Large NB, Widebody) × year
# ─────────────────────────────────────────────────────────────

def build_fact_fleet(dfs: dict) -> pd.DataFrame:
    df_a = dfs["actuals"]
    df_c = dfs["cost_pbh"]

    # Row indices verified against actual file structure
    # Traffic section (absolute row numbers in actuals file):
    # Small NB  →  rows 42-49
    # Large NB  →  rows 90-97
    # Widebody  →  rows 138-145
    fleet_configs = {
        "Small_Narrowbody": {
            "act_bh":42, "act_ah":43, "act_dep":44, "act_rpm":45,
            "act_asm":46, "act_gal":48, "act_days":49,
            "act_acft":30, "act_seats":31, "act_stage":32,
            "act_gph":33, "act_lf":34, "act_dutil":37,
            "act_pilots":5, "act_fuel":12,
            "cost_total":3, "cost_exfuel":4, "cost_fuel":13,
            "cost_pilots":6, "cost_maint":17,
        },
        "Large_Narrowbody": {
            "act_bh":90, "act_ah":91, "act_dep":92, "act_rpm":93,
            "act_asm":94, "act_gal":96, "act_days":97,
            "act_acft":78, "act_seats":79, "act_stage":80,
            "act_gph":81, "act_lf":82, "act_dutil":85,
            "act_pilots":53, "act_fuel":60,
            "cost_total":42, "cost_exfuel":43, "cost_fuel":52,
            "cost_pilots":45, "cost_maint":56,
        },
        "Widebody": {
            "act_bh":138, "act_ah":139, "act_dep":140, "act_rpm":141,
            "act_asm":142, "act_gal":144, "act_days":145,
            "act_acft":126, "act_seats":127, "act_stage":128,
            "act_gph":129, "act_lf":130, "act_dutil":133,
            "act_pilots":101, "act_fuel":108,
            "cost_total":81, "cost_exfuel":82, "cost_fuel":91,
            "cost_pilots":84, "cost_maint":95,
        },
    }

    rows = []
    for ft, cfg in fleet_configs.items():
        for y in YEARS:
            r = {"Year": y, "Fleet_Type": ft}
            r["BlockHours"]       = extract_row(df_a, cfg["act_bh"])[y]
            r["AirborneHours"]    = extract_row(df_a, cfg["act_ah"])[y]
            r["Departures"]       = extract_row(df_a, cfg["act_dep"])[y]
            r["RPM"]              = extract_row(df_a, cfg["act_rpm"])[y]
            r["ASM"]              = extract_row(df_a, cfg["act_asm"])[y]
            r["Gallons"]          = extract_row(df_a, cfg["act_gal"])[y]
            r["AircraftDays"]     = extract_row(df_a, cfg["act_days"])[y]
            r["Aircraft_InFleet"] = extract_row(df_a, cfg["act_acft"])[y]
            r["SeatsPerDep"]      = extract_row(df_a, cfg["act_seats"])[y]
            r["AvgStageLength"]   = extract_row(df_a, cfg["act_stage"])[y]
            r["GalsPerBlockHour"] = extract_row(df_a, cfg["act_gph"])[y]
            r["LoadFactor_Pct"]   = extract_row(df_a, cfg["act_lf"])[y]
            r["DailyUtil_BH"]     = extract_row(df_a, cfg["act_dutil"])[y]
            r["CPBH_Total"]       = extract_row(df_c, cfg["cost_total"])[y]
            r["CPBH_ExFuel"]      = extract_row(df_c, cfg["cost_exfuel"])[y]
            r["CPBH_Fuel"]        = extract_row(df_c, cfg["cost_fuel"])[y]
            r["CPBH_Pilots"]      = extract_row(df_c, cfg["cost_pilots"])[y]
            r["Era"]              = get_era(y)
            rows.append(r)

    df = pd.DataFrame(rows)
    # Filter out years with no data for that fleet type
    df = df[df["BlockHours"].notna() & (df["BlockHours"] > 0)].copy()
    print(f"  fact_fleet_by_type: {df.shape}")
    return df


# ─────────────────────────────────────────────────────────────
# TABLE 3: fact_geography
# Revenue/traffic by geographic region × year
# ─────────────────────────────────────────────────────────────

def build_fact_geography(dfs: dict) -> pd.DataFrame:
    df_t = dfs["traffic"]

    # Row layout in traffic file (absolute row numbers):
    #   Atlantic row 2, LatAm row 8, Pacific row 14,
    #   Domestic row 20, International row 26, Total row 32
    #   Cols within each section: +0 PaxRev, +1 TotalRev, +2 RPM, +3 ASM, +4 Expense
    geo_configs = {
        "Atlantic":      2,
        "LatinAmerica":  8,
        "Pacific":       14,
        "Domestic":      20,
        "International": 26,
        "Total":         32,
    }

    rows = []
    for region, base in geo_configs.items():
        for y in YEARS:
            r = {"Year": y, "Region": region}
            r["PaxRevenue_000"]    = extract_row(df_t, base)[y]
            r["TotalRevenue_000"]  = extract_row(df_t, base + 1)[y]
            r["RPM"]               = extract_row(df_t, base + 2)[y]
            r["ASM"]               = extract_row(df_t, base + 3)[y]
            r["ACOpExpense_000"]   = extract_row(df_t, base + 4)[y]
            # Derived
            r["LoadFactor_Pct"]    = safe_div(r["RPM"], r["ASM"])
            r["Yield_cents"]       = safe_div(
                (r["PaxRevenue_000"] or 0) * 100,
                (r["RPM"] or 0) / 1000
            ) if r["RPM"] else None
            r["RASM_cents"]        = safe_div(
                (r["PaxRevenue_000"] or 0) * 100000,
                r["ASM"]
            ) if r["ASM"] else None
            r["CASM_cents"]        = safe_div(
                (r["ACOpExpense_000"] or 0) * 100000,
                r["ASM"]
            ) if r["ASM"] else None
            r["Era"]               = get_era(y)
            rows.append(r)

    df = pd.DataFrame(rows)
    df = df[df["ASM"].notna() & (df["ASM"] > 0)].copy()
    print(f"  fact_geography: {df.shape}")
    return df


# ─────────────────────────────────────────────────────────────
# TABLE 4: fact_employee_productivity
# Productivity metrics by employee group × year
# ─────────────────────────────────────────────────────────────

def build_fact_employee(dfs: dict) -> pd.DataFrame:
    df_e = dfs["employee"]

    # Employee file row indices per group:
    # BlockHrs/Emp/Mo: rows 19-24, ASMs/Emp(000): rows 27-32
    # Employees: rows 88-102, PaxRev/Emp(000): rows 53-58
    # AvgSalary: rows 63-69, Pax/Emp: rows 43-48
    emp_configs = {
        "Pilots_CoPilots":   {"bh":19,"asm":27,"emp":88,"paxrev":53,"salary":64,"epa":43},
        "Flight_Attendants": {"bh":20,"asm":28,"emp":90,"paxrev":54,"salary":65,"epa":44},
        "Maintenance":       {"bh":21,"asm":29,"emp":91,"paxrev":55,"salary":66,"epa":45},
        "Ground_Handling":   {"bh":22,"asm":30,"emp":92,"paxrev":56,"salary":67,"epa":46},
        "All_Employees":     {"bh":24,"asm":32,"emp":102,"paxrev":58,"salary":69,"epa":48},
    }

    rows = []
    for grp, cfg in emp_configs.items():
        for y in YEARS:
            r = {"Year": y, "Employee_Group": grp}
            r["BlockHrs_per_Emp_Month"] = extract_emp_row(df_e, cfg["bh"])[y]
            r["ASMs_per_Emp_000"]       = extract_emp_row(df_e, cfg["asm"])[y]
            r["Employee_Count"]         = extract_emp_row(df_e, cfg["emp"])[y]
            r["PaxRev_per_Emp_000"]     = extract_emp_row(df_e, cfg["paxrev"])[y]
            r["Avg_Salary"]             = extract_emp_row(df_e, cfg["salary"])[y]
            r["Pax_per_Employee"]       = extract_emp_row(df_e, cfg["epa"])[y]
            r["Era"]                    = get_era(y)
            rows.append(r)

    df = pd.DataFrame(rows)
    df = df[df["Employee_Count"].notna() & (df["Employee_Count"] > 0)].copy()
    print(f"  fact_employee_productivity: {df.shape}")
    return df


# ─────────────────────────────────────────────────────────────
# DIMENSION TABLES
# ─────────────────────────────────────────────────────────────

def build_dimensions() -> dict:
    dim_date = pd.DataFrame({
        "Year":              YEARS,
        "Era":               [get_era(y) for y in YEARS],
        "Era_Order":         [
            1 if y<=2003 else 2 if y<=2009 else 3 if y<=2014 else 4 if y<=2019 else 5
            for y in YEARS
        ],
        "Decade":            [f"{(y//10)*10}s" for y in YEARS],
        "Is_Pre_ULCC":       [1 if y < 2015 else 0 for y in YEARS],
        "Is_COVID":          [1 if y >= 2020 else 0 for y in YEARS],
        "Is_Full_Year":      [0 if y == 2020 else 1 for y in YEARS],
        "Years_Since_Start": [y - 1994 for y in YEARS],
        "Label":             [f"FY{y}" for y in YEARS],
    })

    dim_fleet = pd.DataFrame({
        "Fleet_Type":   ["Small_Narrowbody", "Large_Narrowbody", "Widebody"],
        "Category":     ["Narrowbody", "Narrowbody", "Widebody"],
        "Description":  ["A318/A319 class (~120-140 seats)",
                         "A320/A321 class (~180-220 seats)",
                         "Wide-body aircraft"],
        "Sort_Order":   [1, 2, 3],
        "Dominant_Era": ["1995-2015", "2015-present", "Limited use"],
    })

    dim_region = pd.DataFrame({
        "Region":       ["Atlantic","LatinAmerica","Pacific","Domestic","International","Total"],
        "Region_Group": ["International","International","International","Domestic","International","Total"],
        "Display_Name": ["Atlantic","Latin America","Pacific","Domestic","International (Total)","System Total"],
        "Sort_Order":   [2, 3, 4, 1, 5, 6],
    })

    dim_empgrp = pd.DataFrame({
        "Employee_Group": ["Pilots_CoPilots","Flight_Attendants","Maintenance","Ground_Handling","All_Employees"],
        "Category":       ["Flight Crew","Flight Crew","Technical","Ground Ops","All"],
        "Display_Name":   ["Pilots & Co-Pilots","Flight Attendants","Maintenance","Ground Handling","All Employees"],
        "Sort_Order":     [1, 2, 3, 4, 5],
    })

    print(f"  dim_date: {dim_date.shape}")
    print(f"  dim_fleet_type: {dim_fleet.shape}")
    print(f"  dim_region: {dim_region.shape}")
    print(f"  dim_employee_group: {dim_empgrp.shape}")

    return {
        "dim_date":           dim_date,
        "dim_fleet_type":     dim_fleet,
        "dim_region":         dim_region,
        "dim_employee_group": dim_empgrp,
    }


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Frontier Airlines ETL Pipeline")
    parser.add_argument("--data_dir", default=".", help="Folder containing raw .xls files")
    parser.add_argument("--out_dir",  default="./clean_data", help="Output folder for CSVs")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    print("\n[1/3] Loading raw files...")
    dfs = load_raw_files(args.data_dir)

    print("\n[2/3] Building tables...")
    tables = {
        "fact_annual_operations":      build_fact_annual(dfs),
        "fact_fleet_by_type":          build_fact_fleet(dfs),
        "fact_geography":              build_fact_geography(dfs),
        "fact_employee_productivity":  build_fact_employee(dfs),
        **build_dimensions(),
    }

    print("\n[3/3] Saving CSVs...")
    for name, df in tables.items():
        path = os.path.join(args.out_dir, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"  ✓ {name}.csv  ({df.shape[0]} rows × {df.shape[1]} cols)")

    print(f"\nDone. {len(tables)} files saved to: {args.out_dir}")
    print("\nNext steps:")
    print("  1. Open Power BI Desktop")
    print("  2. Get Data → Folder → select your clean_data/ folder")
    print("  3. Load all 8 CSVs")
    print("  4. In Model view, create relationships:")
    print("     fact_annual_operations[Year]     → dim_date[Year]")
    print("     fact_fleet_by_type[Year]         → dim_date[Year]")
    print("     fact_fleet_by_type[Fleet_Type]   → dim_fleet_type[Fleet_Type]")
    print("     fact_geography[Year]             → dim_date[Year]")
    print("     fact_geography[Region]           → dim_region[Region]")
    print("     fact_employee_productivity[Year] → dim_date[Year]")
    print("     fact_employee_productivity[Employee_Group] → dim_employee_group[Employee_Group]")


if __name__ == "__main__":
    main()
