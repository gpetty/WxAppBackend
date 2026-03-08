"""
Pydantic response models shared across routers.

/forecast returns a plain dict (dynamic variable keys make a fixed Pydantic
model impractical).  Other endpoints use the models defined here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# /variables
# ---------------------------------------------------------------------------

class NativeVariableInfo(BaseModel):
    units:          str
    response_key_suffix: str   # appended to the variable name in /forecast responses
    description:    str
    grib_shortName: str
    fxx_cutoff:     Optional[int] = None


class DerivedVariableInfo(BaseModel):
    units:       str
    response_key_suffix: str
    description: str
    requires:    list[str]


class VariablesResponse(BaseModel):
    """Response body for GET /variables."""
    native:  dict[str, NativeVariableInfo]
    derived: dict[str, DerivedVariableInfo]


# ---------------------------------------------------------------------------
# /status
# ---------------------------------------------------------------------------

class StatusResponse(BaseModel):
    """Response body for GET /status."""
    runtime:          Optional[str]      = Field(None, description="NBM model run (cycle) datetime")
    store_path:       str                = Field(description="Path to the slab ring buffer store")
    n_variables:      int
    n_time_steps:     int                = Field(description="Forecast time steps in current run")
    last_loaded:      Optional[datetime] = Field(None, description="When the store was last opened or reloaded")
    available_cycles: list[str]          = Field(default_factory=list,
                                                 description="Sorted cycle tags in the ring buffer, "
                                                             "e.g. ['20260301_00', '20260301_06']")
