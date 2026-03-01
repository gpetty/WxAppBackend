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
    runtime:      Optional[str]  = Field(None, description="NBM model run (cycle) datetime")
    store_path:   str
    n_variables:  int
    n_time_steps: int
    last_loaded:  datetime
