"""
GET /variables — merged variable catalog for the blend API.

Returns every variable requestable from /blend/forecast, annotated with
which sources it draws from and the merge rule applied.

Note on thunderstorm_probability:
  The blend API exposes this as two separate variables with distinct names:
    - thunderstorm_probability        (NBM general tstm,  response key: *_pct)
    - thunderstorm_probability_severe (NDFD severe tstm,  response key: *_pct)
  They are not merged because they measure different quantities.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

from .blend_models import BlendVariableInfo, BlendVariablesResponse
from .blend_forecast import BLEND_RULES
from .helpers import _suffix

router = APIRouter()


@router.get("/variables", response_model=BlendVariablesResponse)
def get_blend_variables(request: Request) -> BlendVariablesResponse:
    """
    Return the complete variable catalog for the blend API.

    Each entry shows which sources contribute data, the merge rule, and
    the per-source forecast horizon in hours.  Use this to build a variable
    picker UI and to predict the exact response key for each variable.
    """
    state        = request.app.state
    nbm_reg      = state.nbm_registry
    ndfd_reg     = state.ndfd_registry

    catalog: dict[str, BlendVariableInfo] = {}

    for blend_name, rule in BLEND_RULES.items():
        strategy = rule["strategy"]

        # Determine units and description from the appropriate registry
        # (fall back to the other if the primary doesn't have this variable)
        if strategy in ("ndfd_preferred", "ndfd_only"):
            ndfd_var = rule.get("ndfd_var", blend_name)
            reg_var  = ndfd_reg.get(ndfd_var)
            if reg_var is None and strategy == "ndfd_preferred":
                reg_var = nbm_reg.get(rule.get("nbm_var", blend_name))
        elif strategy == "nbm_only":
            nbm_var = rule.get("nbm_var", blend_name)
            reg_var = nbm_reg.get(nbm_var)
        else:  # derived
            reg_var = nbm_reg.get(blend_name) or ndfd_reg.get(blend_name)

        if reg_var is None:
            continue  # safety — should not happen if BLEND_RULES is consistent

        units_out = reg_var.units_out

        # Determine source list
        if strategy == "ndfd_preferred":
            sources = ["ndfd", "nbm"]
        elif strategy == "ndfd_only":
            sources = ["ndfd"]
        elif strategy == "nbm_only":
            sources = ["nbm"]
        else:  # derived
            sources = ["derived"]

        catalog[blend_name] = BlendVariableInfo(
            units               = units_out,
            response_key_suffix = _suffix(units_out),
            description         = reg_var.description,
            sources             = sources,
            merge_rule          = strategy,
            ndfd_horizon_h      = rule.get("ndfd_h"),
            nbm_horizon_h       = rule.get("nbm_h"),
        )

    return BlendVariablesResponse(variables=catalog)
