# Weather Window App — Backend Overview

The backend maintains a continuously fresh, queryable snapshot of weather forecast data for the continental United States. Every three hours it automatically downloads the latest forecasts from two NOAA sources — the National Blend of Models (NBM) and the National Digital Forecast Database (NDFD) — and processes them into an in-memory data store optimized for fast point queries.

When a user's device asks "what is the weather going to be at my location, hour by hour?", the Blend API merges the two sources and responds in milliseconds with a clean, hourly time series covering the next 10–11 days: NDFD data (NWS-authoritative, updated frequently) for days 1–7, and NBM data (extended range) for days 8–11.

The system also accumulates a rolling history of past forecast cycles, so the frontend can overlay earlier forecasts on the same time axis and show how the forecast has evolved over the past several days — the "forecast drift" feature central to the app's design.

A separate archive process saves full forecast time series for a small set of stations after each ingest, building up training data for a future bias-correction scheme.
