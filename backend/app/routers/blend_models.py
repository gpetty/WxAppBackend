"""
Pydantic response models for the /blend endpoints.

These are distinct from models.py because the blend service queries two
independent stores and produces a different response shape.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# /blend/status
# ---------------------------------------------------------------------------

class BlendStatusResponse(BaseModel):
    """Response body for GET /blend/status."""
    nbm_runtime:           Optional[str] = Field(None, description="NBM cycle issue time")
    ndfd_runtime:          Optional[str] = Field(None, description="NDFD cycle issue time")
    nbm_available_cycles:  list[str]     = Field(default_factory=list,
                                                  description="NBM cycle tags in ring buffer")
    ndfd_available_cycles: list[str]     = Field(default_factory=list,
                                                  description="NDFD cycle tags in ring buffer")
    nbm_store_path:        str           = Field(description="Path to NBM slab store")
    ndfd_store_path:       str           = Field(description="Path to NDFD slab store")
    nbm_n_variables:       int
    ndfd_n_variables:      int
    nbm_ready:             bool          = Field(description="NBM store has at least one run")
    ndfd_ready:            bool          = Field(description="NDFD store has at least one run")
    last_loaded:           Optional[datetime] = None


# ---------------------------------------------------------------------------
# /blend/variables
# ---------------------------------------------------------------------------

class BlendVariableInfo(BaseModel):
    """Catalog entry for one variable in the blend API."""
    units:               str
    response_key_suffix: str
    description:         str
    sources:             list[str]     = Field(
        description="Which stores contribute data: ['ndfd', 'nbm'], ['nbm'], etc."
    )
    merge_rule:          str           = Field(
        description="ndfd_preferred | nbm_only | ndfd_only | derived"
    )
    ndfd_horizon_h:      Optional[int] = Field(
        None, description="Hours of NDFD coverage for this variable (null = not from NDFD)"
    )
    nbm_horizon_h:       Optional[int] = Field(
        None, description="Hours of NBM coverage for this variable (null = not from NBM)"
    )


class BlendVariablesResponse(BaseModel):
    """Response body for GET /blend/variables."""
    variables: dict[str, BlendVariableInfo]
