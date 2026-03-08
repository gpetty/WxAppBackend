"""
Per-variable uint8 packing and unpacking.

Encoding (linear):
    stored_uint8 = clip(round((physical - add_offset) / scale_factor), 0, 254)

Decoding:
    physical_float32 = stored_uint8 * scale_factor + add_offset

Missing / sentinel:
    stored value 255 means missing for all variables.
    For cloud_ceiling, 255 additionally means "no ceiling" (unlimited).
    Unpacking 255 yields NaN for all variables except cloud_ceiling → np.inf.

Variable order defines axis k=0..14 in every slab array.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np


# ---------------------------------------------------------------------------
# Canonical variable order — must stay stable once data is on disk
# ---------------------------------------------------------------------------

VARIABLE_ORDER: tuple[str, ...] = (
    "temperature",              # k=0
    "dewpoint",                 # k=1
    "relative_humidity",        # k=2
    "apparent_temperature",     # k=3
    "wind_speed",               # k=4
    "wind_direction",           # k=5
    "wind_gust",                # k=6
    "total_precipitation",      # k=7
    "precip_type",              # k=8
    "thunderstorm_probability", # k=9
    "cape",                     # k=10
    "cloud_cover",              # k=11
    "solar_radiation",          # k=12
    "visibility",               # k=13
    "cloud_ceiling",            # k=14
)

KVARS: int = len(VARIABLE_ORDER)   # 15

MISSING: int = 255   # uint8 sentinel for all variables


@dataclass(frozen=True)
class PackingParams:
    scale_factor: float     # physical = stored * scale_factor + add_offset
    add_offset:   float
    unpack_missing: float   # value to return when stored == MISSING (NaN or inf)


# ---------------------------------------------------------------------------
# Per-variable packing parameters
# ---------------------------------------------------------------------------
#
# Precision summary:
#   temperature / dewpoint / apparent_temperature : 1 °F, range −80..174 °F
#   relative_humidity / cloud_cover / probabilities: 1 %, range 0..100 %
#   wind_speed / wind_gust                         : 1 mph, range 0..254 mph
#   wind_direction                                 : ~1.4°, range 0..360°
#   total_precipitation                            : 0.5 mm, range 0..127 mm
#   precip_type                                    : integer codes 0–9
#   cape                                           : 40 J/kg, range 0..10160 J/kg
#   solar_radiation                                : 6 W/m², range 0..1524 W/m²
#   visibility                                     : 0.1 mi, range 0..25.4 mi
#   cloud_ceiling                                  : 100 ft, range 0..25400 ft;
#                                                    stored 255 = no ceiling → inf
#
# wind_direction uses scale = 360/254 so that stored=254 → 360°.
# All other variables use round numbers for scale_factor.

_NAN = float("nan")
_INF = float("inf")

PACKING: dict[str, PackingParams] = {
    "temperature":              PackingParams(  1.0,  -80.0, _NAN),
    "dewpoint":                 PackingParams(  1.0,  -80.0, _NAN),
    "relative_humidity":        PackingParams(  1.0,    0.0, _NAN),
    "apparent_temperature":     PackingParams(  1.0,  -80.0, _NAN),
    "wind_speed":               PackingParams(  1.0,    0.0, _NAN),
    "wind_direction":           PackingParams(360/254, 0.0,  _NAN),
    "wind_gust":                PackingParams(  1.0,    0.0, _NAN),
    "total_precipitation":      PackingParams(  0.5,    0.0, _NAN),
    "precip_type":              PackingParams(  1.0,    0.0, _NAN),
    "thunderstorm_probability": PackingParams(  1.0,    0.0, _NAN),
    "cape":                     PackingParams( 40.0,    0.0, _NAN),
    "cloud_cover":              PackingParams(  1.0,    0.0, _NAN),
    "solar_radiation":          PackingParams(  6.0,    0.0, _NAN),
    "visibility":               PackingParams(  0.1,    0.0, _NAN),
    "cloud_ceiling":            PackingParams(100.0,    0.0, _INF),  # 255 → no ceiling
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def pack(var_name: str, physical: np.ndarray) -> np.ndarray:
    """
    Pack a float array to uint8 for slab storage.

    NaN and ±inf inputs are stored as 255 (missing sentinel).
    Out-of-range values are clipped to 0–254 before storing.

    Parameters
    ----------
    var_name : str
        Variable name (must be in VARIABLE_ORDER).
    physical : ndarray
        Float array of any shape.

    Returns
    -------
    ndarray of uint8, same shape as input.
    """
    p = PACKING[var_name]
    sentinel_mask = ~np.isfinite(physical)
    # Replace non-finite with 0 before arithmetic to avoid NaN-in-cast warnings;
    # those positions are overwritten with the sentinel below.
    safe = np.where(sentinel_mask, np.float32(0), physical)
    scaled = (safe - p.add_offset) / p.scale_factor
    stored = np.clip(np.round(scaled), 0, 254).astype(np.uint8)
    stored[sentinel_mask] = MISSING
    return stored


def unpack(var_name: str, stored: np.ndarray) -> np.ndarray:
    """
    Unpack a uint8 array to float32 physical values.

    Sentinel 255 is returned as NaN for most variables, or np.inf
    for cloud_ceiling ("no ceiling").

    Parameters
    ----------
    var_name : str
        Variable name (must be in VARIABLE_ORDER).
    stored : ndarray of uint8
        Packed values of any shape.

    Returns
    -------
    ndarray of float32, same shape as input.
    """
    p = PACKING[var_name]
    out = stored.astype(np.float32) * np.float32(p.scale_factor) + np.float32(p.add_offset)
    out[stored == MISSING] = p.unpack_missing
    return out


def packing_table() -> list[dict]:
    """Return a list of dicts describing the packing for each variable, in order."""
    rows = []
    for k, name in enumerate(VARIABLE_ORDER):
        p = PACKING[name]
        rows.append({
            "k":            k,
            "variable":     name,
            "scale_factor": p.scale_factor,
            "add_offset":   p.add_offset,
            "missing_sentinel": MISSING,
            "unpack_missing":   p.unpack_missing,
        })
    return rows
