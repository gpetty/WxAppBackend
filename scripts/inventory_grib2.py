#!/usr/bin/env python3
"""
Inventory all GRIB2 messages in an NBM file and cross-reference against variables.yaml.

Usage:
    python scripts/inventory_grib2.py data/blend.t18z.core.f001.co.grib2

Output:
    - Full field listing (shortName, typeOfLevel, level, units, long name)
    - Cross-reference showing which variables.yaml entries matched / need correction
"""

import sys
import pathlib
import cfgrib
import yaml

VARIABLES_YAML = pathlib.Path(__file__).parent.parent / "backend" / "app" / "variables.yaml"

def inventory(grib2_path: str):
    path = pathlib.Path(grib2_path)
    if not path.exists():
        sys.exit(f"File not found: {path}")

    print(f"\n{'='*80}")
    print(f"  GRIB2 field inventory: {path.name}")
    print(f"{'='*80}\n")

    datasets = cfgrib.open_datasets(str(path))
    print(f"  cfgrib split this file into {len(datasets)} sub-dataset(s)\n")

    # Collect all fields across sub-datasets
    all_fields = []
    for i, ds in enumerate(datasets):
        for var in ds.data_vars:
            a = ds[var].attrs
            all_fields.append({
                "dataset_idx": i,
                "var_key":      var,
                "shortName":    a.get("GRIB_shortName",    "?"),
                "name":         a.get("GRIB_name",         "?"),
                "typeOfLevel":  a.get("GRIB_typeOfLevel",  "?"),
                "level":        a.get("GRIB_level",        "?"),
                "units":        a.get("GRIB_units",        "?"),
                "stepType":     a.get("GRIB_stepType",     "?"),
            })

    # Print full table
    col = "{:<4}  {:<20}  {:<30}  {:<25}  {:<8}  {:<10}  {:<10}"
    print(col.format("DS#", "var_key", "GRIB_name", "typeOfLevel", "level", "shortName", "units"))
    print("-" * 115)
    for f in sorted(all_fields, key=lambda x: (x["typeOfLevel"], x["shortName"])):
        print(col.format(
            f["dataset_idx"],
            f["var_key"][:20],
            f["name"][:30],
            f["typeOfLevel"][:25],
            str(f["level"])[:8],
            f["shortName"][:10],
            f["units"][:10],
        ))

    # Cross-reference against variables.yaml if it exists
    if VARIABLES_YAML.exists():
        print(f"\n{'='*80}")
        print(f"  Cross-reference: variables.yaml vs. discovered fields")
        print(f"{'='*80}\n")

        with open(VARIABLES_YAML) as f:
            registry = yaml.safe_load(f).get("variables", {})

        field_index = {
            (f["shortName"], f["typeOfLevel"]): f for f in all_fields
        }

        for name, cfg in registry.items():
            if cfg.get("derived"):
                print(f"  [DERIVED ]  {name}")
                continue
            key = (cfg.get("grib_shortName", ""), cfg.get("grib_typeOfLevel", ""))
            match = field_index.get(key)
            if match:
                print(f"  [OK      ]  {name:25s}  shortName={key[0]:12s}  typeOfLevel={key[1]}")
            else:
                print(f"  [MISSING ]  {name:25s}  shortName={key[0]:12s}  typeOfLevel={key[1]}  <-- needs correction")
    else:
        print(f"\n  (No variables.yaml found at {VARIABLES_YAML} — skipping cross-reference)")

    print()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python scripts/inventory_grib2.py <path/to/file.grib2>")
    inventory(sys.argv[1])
