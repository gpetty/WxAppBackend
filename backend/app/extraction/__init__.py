"""
extraction — query layer for the NBM slab ring buffer.

Public API::

    from backend.app.extraction import query_forecast, find_nearest_grid_point

    df, actual_lat, actual_lon = query_forecast(
        store=store,
        lat_grid=lat_grid, lon_grid=lon_grid,
        lat=43.07, lon=-89.40,
        variables=["temperature", "wind_speed", "sun_elevation"],
        registry=registry,
    )
"""

from .slab_query import find_nearest_grid_point, query_forecast

__all__ = ["find_nearest_grid_point", "query_forecast"]
