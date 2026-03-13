"""
NDFDVariableRegistry — loads and validates variables_ndfd.yaml.

Parallel to registry.py but adds NDFD-specific fields:
  - s3_element    : element abbreviation (ds.{element}.bin)
  - period_cutoff : period at which this element is available (VP.001-003 or None)

The existing NativeVariable / DerivedVariable dataclasses are reused for the
API layer (routers and slab_query); NDFDNativeVariable extends NativeVariable
with the NDFD-specific fields needed by the post-processor.
"""

from __future__ import annotations

import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from .registry import DerivedVariable   # reuse DerivedVariable unchanged


@dataclass
class NDFDNativeVariable:
    """A weather variable sourced from an NDFD element file."""
    name:             str
    s3_element:       str            # ds.{s3_element}.bin
    cfgrib_var_key:   str
    grib_shortName:   str
    grib_typeOfLevel: str
    units_raw:        str
    units_out:        str
    description:      str
    period_cutoff:    Optional[str] = None  # e.g. "VP.001-003"; None = all periods


class NDFDVariableRegistry:
    """
    Loads variables_ndfd.yaml and provides typed access to NDFD variables.

    Exposes the same interface as VariableRegistry so it can be passed to
    the slab ingest pipeline.  For the API layer, the standard VariableRegistry
    (which ignores NDFD-specific fields) is used instead.
    """

    _REQUIRED_NATIVE = {
        "s3_element", "cfgrib_var_key", "grib_shortName",
        "grib_typeOfLevel", "units_raw", "units_out", "description",
    }
    _REQUIRED_DERIVED = {"requires", "formula", "units_out", "description"}

    def __init__(self, yaml_path: Path) -> None:
        with open(yaml_path) as fh:
            raw = yaml.safe_load(fh)

        self._native:  Dict[str, NDFDNativeVariable] = {}
        self._derived: Dict[str, DerivedVariable]    = {}

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
                    raise ValueError(f"NDFD native variable '{name}' missing fields: {missing}")
                self._native[name] = NDFDNativeVariable(
                    name             = name,
                    s3_element       = cfg["s3_element"],
                    cfgrib_var_key   = cfg["cfgrib_var_key"],
                    grib_shortName   = cfg["grib_shortName"],
                    grib_typeOfLevel = cfg["grib_typeOfLevel"],
                    units_raw        = cfg["units_raw"],
                    units_out        = cfg["units_out"],
                    description      = cfg["description"],
                    period_cutoff    = cfg.get("period_cutoff"),
                )

    def native(self) -> Dict[str, NDFDNativeVariable]:
        return dict(self._native)

    def derived(self) -> Dict[str, DerivedVariable]:
        return dict(self._derived)

    def get(self, name: str) -> Optional[NDFDNativeVariable | DerivedVariable]:
        return self._native.get(name) or self._derived.get(name)

    def all_names(self) -> List[str]:
        return list(self._native) + list(self._derived)

    def __repr__(self) -> str:
        return (f"NDFDVariableRegistry({len(self._native)} native, "
                f"{len(self._derived)} derived)")
