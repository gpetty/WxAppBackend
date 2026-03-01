"""
Unit conversions applied during GRIB2 → Zarr post-processing.

Each function operates on numpy arrays (or xarray DataArrays) element-wise.
Conversions are keyed by (units_raw, units_out) pairs from variables.yaml.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

import numpy as np

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Conversion functions  (all operate on array-like inputs)
# ---------------------------------------------------------------------------

def kelvin_to_fahrenheit(arr):
    """K → F"""
    return (arr - 273.15) * 9.0 / 5.0 + 32.0


def mps_to_mph(arr):
    """m/s → mph  (also handles cfgrib's 'm s**-1' notation)"""
    return arr * 2.23694


def metres_to_miles(arr):
    """m → miles"""
    return arr / 1609.344


def metres_to_feet(arr):
    """m → feet"""
    return arr * 3.28084


def identity(arr):
    """No-op: units are already correct."""
    return arr


# ---------------------------------------------------------------------------
# Registry: maps (units_raw, units_out) → conversion function
#
# Normalise unit strings to lowercase for matching so that minor casing
# differences in variables.yaml don't cause lookup failures.
# ---------------------------------------------------------------------------

_CONVERSION_TABLE: dict[tuple[str, str], Callable] = {
    ("k",          "f"):       kelvin_to_fahrenheit,
    ("m s**-1",    "mph"):     mps_to_mph,
    ("m",          "miles"):   metres_to_miles,
    ("m",          "feet"):    metres_to_feet,
}

# Identity pairs (same in, same out — no conversion needed)
_IDENTITY_PAIRS = [
    ("%", "%"),
    ("w m**-2", "w m**-2"),
    ("j kg**-1", "j kg**-1"),
    ("kg m**-2", "kg m**-2"),
    ("kg m**-2", "mm"),   # 1 kg/m² water = 1 mm liquid-equivalent (ρ_water = 1000 kg/m³)
    ("degree true", "degree true"),
    ("(code table 4.201)", "(code table 4.201)"),
    ("degrees", "degrees"),
]
for pair in _IDENTITY_PAIRS:
    _CONVERSION_TABLE[pair] = identity


def get_converter(units_raw: str, units_out: str) -> Callable:
    """
    Return the conversion function for the given (units_raw, units_out) pair.

    Raises KeyError if no matching conversion is registered. Add new
    conversions to _CONVERSION_TABLE above.
    """
    key = (units_raw.lower().strip(), units_out.lower().strip())

    if key[0] == key[1]:
        return identity

    fn = _CONVERSION_TABLE.get(key)
    if fn is None:
        raise KeyError(
            f"No unit conversion registered for {units_raw!r} → {units_out!r}. "
            f"Add an entry to _CONVERSION_TABLE in conversions.py."
        )
    return fn
