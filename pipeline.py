import pandas as pd
from pathlib import Path
import unicodedata
import re

# =====================================================================================
# FAO Data Pipeline
# =====================================================================================
# This pipeline generates a Tableau-ready CSV file from the tables found in the FAO official site: 
# https://www.fao.org/faostat/en/#data/QCL
# =====================================================================================

# Define base path
base_dir = Path(__file__).resolve().parent

# File paths
all_data_path = base_dir / 'Production_Crops_Livestock_E_All_Data_(Normalized).csv'
area_codes_path = base_dir / 'Production_Crops_Livestock_E_AreaCodes.csv'
item_codes_path = base_dir / 'Production_Crops_Livestock_E_ItemCodes.csv'
element_codes_path = base_dir / 'Production_Crops_Livestock_E_Elements.csv'
flags_path = base_dir / 'Production_Crops_Livestock_E_Flags.csv'

# Load data
df_base = pd.read_csv(all_data_path, dtype=str)
df_area = pd.read_csv(area_codes_path, dtype=str)
df_item = pd.read_csv(item_codes_path, dtype=str)
df_element = pd.read_csv(element_codes_path, dtype=str)
df_flag = pd.read_csv(flags_path, dtype=str)

# Normalize column names
def normalize_columns(df):
    df.columns = [unicodedata.normalize("NFKD", col.strip())
                  .encode("ascii", "ignore")
                  .decode("ascii")
                  .replace(" ", "_")
                  for col in df.columns]
    return df

df_base = normalize_columns(df_base)
df_area = normalize_columns(df_area)
df_item = normalize_columns(df_item)
df_element = normalize_columns(df_element)
df_flag = normalize_columns(df_flag)

# Normalize key values
def normalize_keys(df, keys):
    for key in keys:
        df[key] = df[key].astype(str).str.replace(r"[^\x00-\x7F]+", "", regex=True)
        df[key] = df[key].str.strip().str.lstrip("0")
    return df

df_base = normalize_keys(df_base, ["Area_Code", "Item_Code", "Element_Code"])
df_area = normalize_keys(df_area, ["Area_Code"])
df_item = normalize_keys(df_item, ["Item_Code"])
df_element = normalize_keys(df_element, ["Element_Code"])

# Validate columns
def validate_columns(df, expected, name):
    missing = set(expected) - set(df.columns)
    if missing:
        raise ValueError(f"{name} missing columns: {missing}")

validate_columns(df_base, ["Area_Code", "Item_Code", "Element_Code", "Year", "Value"], "Base")
validate_columns(df_area, ["Area_Code", "Area"], "Area")
validate_columns(df_item, ["Item_Code", "Item"], "Item")
validate_columns(df_element, ["Element_Code", "Element"], "Element")
validate_columns(df_flag, ["Flag", "Description"], "Flag")

# Merge with explicit renaming
df = df_base.copy()
df = df.merge(df_area.rename(columns={"Area": "Area_Name"})[["Area_Code", "Area_Name"]], on="Area_Code", how="left", validate="many_to_one")
df = df.merge(df_item.rename(columns={"Item": "Item_Name"})[["Item_Code", "Item_Name"]], on="Item_Code", how="left", validate="many_to_one")
df = df.merge(df_element.rename(columns={"Element": "Element_Name"})[["Element_Code", "Element_Name"]], on="Element_Code", how="left", validate="many_to_one")

# Null check
def assert_no_nulls(df, cols, name):
    nulls = df[cols].isnull().sum()
    if nulls.any():
        raise ValueError(f"{name} contains nulls in joined columns:\n{nulls}")

assert_no_nulls(df, ["Area_Name", "Item_Name", "Element_Name"], "Post-Merge")

# Filter
df = df[df["Element_Name"].isin(["Yield", "Production", "Area harvested"])]
df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
df = df[df["Year"].between(2005, 2022)]
df["Value"] = pd.to_numeric(df["Value"], errors="coerce")

# Validate shape
if df["Year"].min() < 2005 or df["Year"].max() > 2022:
    raise ValueError("Year range out of bounds")
if df["Value"].isnull().all():
    raise ValueError("All values are NaN")

# Pivot
pivot_input = df[["Area_Name", "Item_Name", "Year", "Element_Name", "Value"]].dropna()
df_pivot = pivot_input.pivot_table(
    index=["Area_Name", "Item_Name", "Year"],
    columns="Element_Name",
    values="Value",
    aggfunc="first"
).reset_index()

df_pivot.columns.name = None
df_pivot.columns = [str(col).strip() for col in df_pivot.columns]

# Rename
df_pivot = df_pivot.rename(columns={
    "Area_Name": "Country",
    "Item_Name": "Crop",
    "Yield": "Yield_tonha",
    "Production": "Production_tons",
    "Area harvested": "AreaHarvested_ha"
})

# Export
output_path = base_dir / 'FAO_Crop_Yield_TableauReady.csv'
df_pivot.to_csv(output_path, index=False)

# Validate output
df_check = pd.read_csv(output_path)
expected_cols = ["Country", "Crop", "Year", "Yield_tonha", "Production_tons", "AreaHarvested_ha"]
missing = set(expected_cols) - set(df_check.columns)
if missing:
    raise ValueError(f"Final file missing columns: {missing}")

print("Pipeline completed. File saved at:", output_path)
