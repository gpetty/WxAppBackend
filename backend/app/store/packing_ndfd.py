"""
Per-variable uint8 packing and unpacking for the NDFD slab store.

Parallel to packing.py (NBM), but uses the NDFD variable set:
  - Drops: cape, solar_radiation, precip_type (wx encoding too complex for v1)
  - Adds:  wet_bulb_globe_temp (wbgt), snowfall
  - Keeps: all other variables with identical packing params

Encoding (linear):
    stored_uint8 = clip(round((physical - add_offset) / scale_factor), 0, 254)

Decoding:
    physical_float32 = stored_uint8 * scale_factor + add_offset

Missing / sentinel:
    stored value 255 means missing for all variables.
    For cloud_ceiling, 255 additionally means "no ceiling" (unlimited) → np.inf.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


# ---------------------------------------------------------------------------
# Canonical variable order — must stay stable once data is on disk
# ---------------------------------------------------------------------------

NDFD_VARIABLE_ORDER: tuple[str, ...] = (
    "temperature",              # k=0   ds.temp.bin   (2t,     K→F)
    "dewpoint",                 # k=1   ds.td.bin     (2d,     K→F)
    "relative_humidity",        # k=2   ds.rhm.bin    (2r,     %)
    "apparent_temperature",     # k=3   ds.apt.bin    (unknown,K→F)  both periods
    "wind_speed",               # k=4   ds.wspd.bin   (10si,   m/s→mph)
    "wind_direction",           # k=5   ds.wdir.bin   (10wdir, deg)
    "wind_gust",                # k=6   ds.wgust.bin  (i10fg,  m/s→mph)
    "total_precipitation",      # k=7   ds.qpf.bin    (unknown,kg/m²=mm)  VP.001-003 only
    "thunderstorm_probability", # k=8   ds.ptotsvrtstm.bin (unknown, %)
    "cloud_cover",              # k=9   ds.sky.bin    (unknown,%)
    "visibility",               # k=10  ds.vis.bin    (vis,    m→mi)   VP.001-003 only
    "cloud_ceiling",            # k=11  ds.cig.bin    (ceil,   surface, m→ft)  VP.001-003 only
    "wet_bulb_globe_temp",      # k=12  ds.wbgt.bin   (unknown,K→F)
    "snowfall",                 # k=13  ds.snow.bin   (unknown,kg/m²=mm)  VP.001-003 only
)

NDFD_KVARS: int = len(NDFD_VARIABLE_ORDER)   # 14

MISSING: int = 255   # uint8 sentinel for all variables


@dataclass(frozen=True)
class PackingParams:
    scale_factor:   float   # physical = stored * scale_factor + add_offset
    add_offset:     float
    unpack_missing: float   # value to return when stored == MISSING


_NAN = float("nan")
_INF = float("inf")

NDFD_PACKING: dict[str, PackingParams] = {
    # Temperatures — 1 °F, range −80..174 °F
    "temperature":              PackingParams(  1.0,  -80.0, _NAN),
    "dewpoint":                 PackingParams(  1.0,  -80.0, _NAN),
    "apparent_temperature":     PackingParams(  1.0,  -80.0, _NAN),
    "wet_bulb_globe_temp":      PackingParams(  1.0,  -80.0, _NAN),

    # Humidity / cloud / probability — 1 %, range 0..100 %
    "relative_humidity":        PackingParams(  1.0,    0.0, _NAN),
    "cloud_cover":              PackingParams(  1.0,    0.0, _NAN),
    "thunderstorm_probability": PackingParams(  1.0,    0.0, _NAN),

    # Wind — 1 mph, range 0..254 mph
    "wind_speed":               PackingParams(  1.0,    0.0, _NAN),
    "wind_gust":                PackingParams(  1.0,    0.0, _NAN),

    # Wind direction — ~1.4°, range 0..360°
    "wind_direction":           PackingParams(360/254, 0.0,  _NAN),

    # Precipitation — 0.5 mm, range 0..127 mm liquid-equivalent
    "total_precipitation":      PackingParams(  0.5,    0.0, _NAN),
    "snowfall":                 PackingParams(  0.5,    0.0, _NAN),

    # Visibility — 0.1 mi, range 0..25.4 mi
    "visibility":               PackingParams(  0.1,    0.0, _NAN),

    # Cloud ceiling — 100 ft, range 0..25400 ft; 255 = no ceiling → inf
    "cloud_ceiling":            PackingParams(100.0,    0.0, _INF),
}


# ---------------------------------------------------------------------------
# Public API (mirrors packing.py interface)
# ---------------------------------------------------------------------------

def ndfd_pack(var_name: str, physical: np.ndarray) -> np.ndarray:
    """
    Pack a float array to uint8 for slab storage.

    NaN and ±inf inputs are stored as 255 (missing sentinel).
    Out-of-range values are clipped to 0–254 before storing.
    """
    p = NDFD_PACKING[var_name]
    sentinel_mask = ~np.isfinite(physical)
    safe = np.where(sentinel_mask, np.float32(0), physical)
    scaled = (safe - p.add_offset) / p.scale_factor
    stored = np.clip(np.round(scaled), 0, 254).astype(np.uint8)
    stored[sentinel_mask] = MISSING
    return stored


def ndfd_unpack(var_name: str, stored: np.ndarray) -> np.ndarray:
    """
    Unpack a uint8 array to float32 physical values.

    Sentinel 255 is returned as NaN for most variables, or np.inf
    for cloud_ceiling ("no ceiling / unlimited").
    """
    p = NDFD_PACKING[var_name]
    out = stored.astype(np.float32) * np.float32(p.scale_factor) + np.float32(p.add_offset)
    out[stored == MISSING] = p.unpack_missing
    return out


def ndfd_packing_table() -> list[dict]:
    """Return a list of dicts describing the packing for each variable, in order."""
    rows = []
    for k, name in enumerate(NDFD_VARIABLE_ORDER):
        p = NDFD_PACKING[name]
        rows.append({
            "k":                k,
            "variable":         name,
            "scale_factor":     p.scale_factor,
            "add_offset":       p.add_offset,
            "missing_sentinel": MISSING,
            "unpack_missing":   p.unpack_missing,
        })
    return rows
