"""
extraction — query layer for the NBM Zarr store.

Public API::

    from backend.app.extraction import open_zarr_store, query_forecast

    ds  = open_zarr_store(zarr_path)          # once at startup
    df, actual_lat, actual_lon = query_forecast(
        ds, lat=43.07, lon=-89.40,
        variables=["temperature", "wind_speed", "sun_elevation"],
        registry=registry,
    )
"""

from .zarr_query import open_zarr_store, query_forecast

__all__ = ["open_zarr_store", "query_forecast"]
