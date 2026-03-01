"""
GET /variables — return the full variable registry.

Each entry includes the `response_key_suffix` so clients can predict the
exact key they will receive in /forecast without hard-coding the mapping.
For example, requesting "temperature" returns the key "temperature_F".
"""

from __future__ import annotations

from fastapi import APIRouter, Request

from ..registry import NativeVariable, DerivedVariable
from .forecast import _UNIT_SUFFIX, _response_key
from .models import NativeVariableInfo, DerivedVariableInfo, VariablesResponse

router = APIRouter()


def _suffix(units_out: str) -> str:
    return _UNIT_SUFFIX.get(units_out, "")


@router.get("/variables", response_model=VariablesResponse)
def get_variables(request: Request) -> VariablesResponse:
    """
    Return all supported variable names, units, and the response key suffix
    used in /forecast.  Request variables by their plain name (e.g.
    'temperature'); receive results under the suffixed key ('temperature_F').
    """
    registry = request.app.state.registry

    native = {
        name: NativeVariableInfo(
            units=var.units_out,
            response_key_suffix=_suffix(var.units_out),
            description=var.description,
            grib_shortName=var.grib_shortName,
            fxx_cutoff=var.fxx_cutoff,
        )
        for name, var in registry.native().items()
    }

    derived = {
        name: DerivedVariableInfo(
            units=var.units_out,
            response_key_suffix=_suffix(var.units_out),
            description=var.description,
            requires=var.requires,
        )
        for name, var in registry.derived().items()
    }

    return VariablesResponse(native=native, derived=derived)
