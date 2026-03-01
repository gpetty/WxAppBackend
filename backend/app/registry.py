"""
VariableRegistry — loads and validates variables.yaml.

Usage:
    from backend.app.registry import VariableRegistry
    from backend.app.config import VARIABLES_YAML

    reg = VariableRegistry(VARIABLES_YAML)
    for name, var in reg.native().items():
        print(name, var.grib_shortName)
"""

from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class NativeVariable:
    """A weather variable extracted directly from NBM GRIB2 files."""
    name:              str
    cfgrib_var_key:    str            # xarray variable name assigned by cfgrib
    grib_shortName:    str            # GRIB2 shortName as reported by cfgrib
    grib_typeOfLevel:  str            # GRIB2 typeOfLevel string
    units_raw:         str            # units as stored in the GRIB2 file
    units_out:         str            # units returned by the API
    description:       str
    fxx_availability:  Optional[List[int]] = None  # None = present at every forecast hour
    fxx_cutoff:        Optional[int] = None         # max fxx at which field is present (None = no cutoff)
    grib_accum_hours:  Optional[int] = None         # if set, filter GRIB extraction to this exact accumulation window


@dataclass
class DerivedVariable:
    """A weather variable computed from one or more native variables."""
    name:        str
    requires:    List[str]            # names of native variables needed
    formula:     str                  # identifier for the computation function
    units_out:   str
    description: str
    valid_when:  Optional[str] = None # human-readable applicability note


class VariableRegistry:
    """
    Loads variables.yaml and provides typed access to native and derived variables.
    Raises ValueError on load if any required YAML fields are missing.
    """

    _REQUIRED_NATIVE  = {"cfgrib_var_key", "grib_shortName", "grib_typeOfLevel",
                          "units_raw", "units_out", "description"}
    _REQUIRED_DERIVED = {"requires", "formula", "units_out", "description"}

    def __init__(self, yaml_path: Path) -> None:
        with open(yaml_path) as fh:
            raw = yaml.safe_load(fh)

        self._native:  Dict[str, NativeVariable]  = {}
        self._derived: Dict[str, DerivedVariable] = {}

        for name, cfg in raw.get("variables", {}).items():
            if cfg.get("derived"):
                missing = self._REQUIRED_DERIVED - set(cfg)
                if missing:
                    raise ValueError(f"Derived variable '{name}' missing fields: {missing}")
                self._derived[name] = DerivedVariable(
                    name        = name,
                    requires    = cfg["requires"],
                    formula     = cfg["formula"],
                    units_out   = cfg["units_out"],
                    description = cfg["description"],
                    valid_when  = cfg.get("valid_when"),
                )
            else:
                missing = self._REQUIRED_NATIVE - set(cfg)
                if missing:
                    raise ValueError(f"Native variable '{name}' missing fields: {missing}")
                self._native[name] = NativeVariable(
                    name             = name,
                    cfgrib_var_key   = cfg["cfgrib_var_key"],
                    grib_shortName   = cfg["grib_shortName"],
                    grib_typeOfLevel = cfg["grib_typeOfLevel"],
                    units_raw        = cfg["units_raw"],
                    units_out        = cfg["units_out"],
                    description      = cfg["description"],
                    fxx_availability = cfg.get("fxx_availability"),
                    fxx_cutoff       = cfg.get("fxx_cutoff"),
                    grib_accum_hours = cfg.get("grib_accum_hours"),
                )

    def native(self) -> Dict[str, NativeVariable]:
        """All native (GRIB2-sourced) variables keyed by name."""
        return dict(self._native)

    def derived(self) -> Dict[str, DerivedVariable]:
        """All derived (computed) variables keyed by name."""
        return dict(self._derived)

    def get(self, name: str) -> Optional[NativeVariable | DerivedVariable]:
        """Look up any variable by name. Returns None if not found."""
        return self._native.get(name) or self._derived.get(name)

    def all_names(self) -> List[str]:
        return list(self._native) + list(self._derived)

    def __repr__(self) -> str:
        return (f"VariableRegistry({len(self._native)} native, "
                f"{len(self._derived)} derived)")
