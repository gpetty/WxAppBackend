# NBM Variable Mapping: Web Docs vs. Our GRIB2 Inventory

**Reference:** [NBM Weather Element Definitions](https://vlab.noaa.gov/web/mdl/nbm-weather-elements) (MDL, NBM v4.2)
**Verified against:** `blend.t18z.core.f{001,037,196}.co.grib2`
**Date verified:** 2026-02-28

---

## Variables We Capture (native)

| Our name | NBM element | NBM abbrev | cfgrib shortName | Documented fxx range | Our actual fxx range | GRIB2 units | Output units | Notes |
|---|---|---|---|---|---|---|---|---|
| `temperature` | 2-m Temperature | Temp | `2t` | hourly to 36h, 3-hr to 192h, 6-hr to 264h | **f001–f264** (all segments) | K | °F | Matches docs exactly |
| `dewpoint` | 2-m Dewpoint | Td | `2d` | same as Temp | **f001–f264** | K | °F | Matches docs exactly |
| `relative_humidity` | 2-m Relative Humidity | RH | `2r` | derived from T / Td; same schedule | **f001–f264** | % (0–100) | % | Present natively in files despite being "derived" in docs |
| `apparent_temperature` | *Not listed in element table* | aptmp | `aptmp` | — | **f001–f264** | K | °F | NBM-native blended feels-like; supersedes our derived heat_index/wind_chill; absent from element definitions page but present in every file |
| `wind_speed` | 10-m Wind Speed | 10-m Speed | `10si` | hourly to 36h, 3-hr to 192h, 6-hr to 264h | **f001–f264** | m s⁻¹ | mph | Matches docs |
| `wind_direction` | 10-m Wind Direction | 10-m Direction | `10wdir` | same schedule | **f001–f264** | degrees (met. convention, 0–360) | degrees | Matches docs |
| `wind_gust` | 10-m Wind Gust | 10-m Gust | `i10fg` | same schedule | **f001–f264** | m s⁻¹ | mph | Matches docs |
| `total_precipitation` | 1-hr QPF | QPF01 | `tp` | "Through 264 hours" | **f001–f264** | kg m⁻² (= mm liquid equivalent) | mm | 1-hr accumulation at every fxx; file schedule (1/3/6-hr) affects temporal density, not accumulation window. See note below. |
| `precip_probability` | *12-hr PoP* | pop12 | `pop12` | Docs show PoP01 only to 36h; pop12 not listed | 6-hr boundary fxx only (~44 files) | % (0–100) | % | 0 extractions in standard core files; effectively not usable for quasi-hourly app |
| `precip_type` | Precipitation Type | PType | `ptype` | Instantaneous; all fxx | **f001–f264** (unverified past f196) | dimensionless integer (GRIB2 code table 4.201) | integer code | Rain=1, Freezing rain=3, Snow=5, Sleet/ice pellets=8 |
| `thunderstorm_probability` | Prob. of Thunder (1-hr) | PoT01 | `tstm` | Docs: **"through 36 hours"** | **f001–f190** (with gaps) | % (0–100) | % | ⚠️ **See note below — major discrepancy with docs** |
| `cape` | CAPE (aviation suite) | CAPE | `cape` | Docs mention under aviation; no explicit fxx range given | **f001–f264** (assumed; unverified) | J kg⁻¹ | J kg⁻¹ | Listed alongside echo tops (36h hourly) — actual range needs verification |
| `cloud_cover` | Mean Sky Cover | Cover | `tcc` | hourly to 36h, 3-hr to 192h, 6-hr to 264h | **f001–f264** | % (0–100) | % | Matches docs |
| `solar_radiation` | *Not in main element table* | sdswrf | `sdswrf` | Fire weather support variable; no explicit range | **f001–f264** (assumed) | W m⁻² | W m⁻² | Used in Fosberg Fire Index calculation; present in files but not documented as a standalone element |
| `visibility` | Visibility | Vis | `vis` | Docs: expert-weighted; no explicit fxx cutoff given | **f001–f076** hard cutoff | m | miles | ⚠️ Docs don't acknowledge this cutoff; likely a modeling limitation |
| `cloud_ceiling` | Ceiling | Cig | `ceil` | Docs: no explicit fxx cutoff given | **f001–f082** hard cutoff | m | feet | ⚠️ Docs don't acknowledge this cutoff |

---

## ⚠️ Critical Notes

### `total_precipitation` (tp) — QPF01: consistently 1-hour accumulation throughout

The NBM documentation says QPF01 is "hourly rainfall or melted liquid equivalent...through 264 hours." The label **QPF01 means 1-hour accumulation at every fxx step**, regardless of the file output schedule.

This is deliberate by design: the NBM is a post-processed blend, not raw model output. Even in the 3-hourly segment the NBM produces its best estimate of *how much rain falls in that specific valid hour* — not a 3-hour bucket. You get fewer valid times (every 3rd hour rather than every hour), but each value you do get is still a 1-hour QPF.

Evidence: the NBM publishes **QPF06** (6-hour accumulation, through 264h) as a **separate, distinctly named product**. If `tp` became a 3-hour accumulation in the 3-hourly segment, it would be QPF03 — and NOAA would name and document it separately, as they do for QPF06. The fact that only QPF01 and QPF06 exist (no QPF03) is strong evidence that `tp` remains a 1-hour accumulation at all fxx.

**Practical implication:** `total_precipitation` values across all three fxx segments are directly comparable — each represents approximately "how much rain falls in this hour." In the 3-hourly segment you simply have gaps (you know the f039 hour but not f038 or f037), and in the 6-hourly segment larger gaps. For a quasi-hourly UI, display precip at the valid times present and accept the gaps as an inherent limitation of the extended-range forecast.

**Action item (low urgency):** Verify by inspecting `stepRange` on a sample f037 `tp` field to confirm it encodes a 1-hour interval (e.g. `"38-39"`) rather than `"37-40"`. This would remove any remaining ambiguity.

The NBM separately publishes **QPF06** (6-hour accumulation, through 264h) — a distinct product we do not currently extract and likely don't need for a quasi-hourly app.

---

### `thunderstorm_probability` (tstm) — Major discrepancy with docs

The MDL documentation describes PoT01 as a **"1-hour Probability of Thunder through 36 hours."** However we have empirically verified that `tstm` is present in GRIB2 files through **f190**, albeit with a changing pattern:

| fxx range | Behavior |
|---|---|
| f001–f082 | Present at every hourly/3-hourly step |
| f085–f082 | Present only at every-6-hour boundaries (f088, f094, …, f190) |
| f196+ | Absent |

This suggests the NBM encodes two variants of thunderstorm probability:
- **PoT01** (the product documented on the web page): 1-hour window, hourly resolution, to f036 only
- A **longer-range tstm product** (possibly 3-hr or 6-hr window): in the same `tstm` shortName slot, extending to f190

The web documentation almost certainly describes only the primary PoT01 product. The longer-range version may have different skill and should be used with caution. **The time window encoded in the GRIB2 `stepRange` field should be verified** to confirm whether the f037+ tstm is a 3-hour or 6-hour probability window.

---

### `apparent_temperature` (aptmp) — Unlisted but valuable

The MDL element definitions page does not list `aptmp` as a standalone element, yet it is present in every file through f264. It is a blended NBM product that accounts for heat index and wind chill depending on conditions. Since it is pre-computed by the NBM's blending system (rather than derived by us), it is likely **more accurate than our hand-computed `heat_index` and `wind_chill` derived variables.** Consider making it the primary feels-like output and demoting our derived versions to fallbacks or removing them.

---

## NBM Elements We Do NOT Currently Capture

The following are available in the NBM CONUS core product but not in `variables.yaml`. Listed with assessment of relevance for a quasi-hourly weather app:

| NBM element | Abbrev | Docs fxx range | Priority | Notes |
|---|---|---|---|---|
| Max Temperature | MaxT | 00z–18z window; 264h | Low | Daily extremes; not quasi-hourly |
| Min Temperature | MinT | 12z–06z window; 264h | Low | Daily extremes; not quasi-hourly |
| 1-hr QPF | QPF01 | Through 264h | Medium | We get this as `tp` in hourly files; worth flagging the window issue |
| 6-hr QPF | QPF06 | Through 264h | Medium | Useful for extended range; not currently extracted |
| 1-hr PoP | PoP01 | **36 hours only** | Medium | Short-range precip probability; more useful than pop12 for a quasi-hourly app |
| 1-hr Snow Accumulation | SnowAmt01 | Through 36h | Medium | Relevant for winter UX |
| 6-hr Snow Accumulation | SnowAmt06 | Through 264h | Low-Medium | Extended range winter precip |
| 1-hr Ice Accumulation | IceAccum01 | Through 36h | Low | Niche use case |
| Precipitation Type (prob.) | PType | All fxx | — | We have this as `precip_type` ✓ |
| 18-dBZ Echo Tops | EchoTops | 36h hourly | Low | Aviation/radar proxy; niche |
| Heat Index (native) | hindex | f196+ only (6-hourly) | Low | Native NBM product but only available in 6-hourly segment; our computed version is better for quasi-hourly |
| Daily Max Temp | tmax | f196+ | Low | Summarized; not quasi-hourly |
| Snowfall Water Equiv. | sf | f196+ | Low | Winter extension |
| Haines Fire Index | Haines | 264h | Low | Fire weather niche |
| Fosberg Fire Index | Fosberg | 264h | Low | Fire weather niche |

---

## Recommendations

1. **Optionally verify `tp` stepRange** on a sample f037 file to confirm the GRIB2 interval is 1-hour (e.g. `stepRange="38-39"`). Low priority — the QPF01 naming and the absence of a QPF03 product strongly imply it is always 1-hr accumulation.

2. **Verify `tstm` step range** on an f088 sample file to confirm whether it encodes a 3-hour or 6-hour thunder probability window for that segment.

3. **Consider adding `PoP01`** (1-hr precip probability, shortName likely `pop` or `pop01`) for the f001–f036 hourly segment. This is more meaningful for a quasi-hourly app than `pop12`.

4. **Demote or remove `precip_probability` (pop12)** — currently produces 0 extractions in standard core files. Confirmed not useful for quasi-hourly app.

5. **Consider `apparent_temperature` as primary feels-like output** over our derived heat_index/wind_chill — it's an NBM-native blended product likely with better calibration.

6. **Consider `SnowAmt01`** for Phase 2 — hourly snow accumulation through 36h would add meaningful value to a quasi-hourly UX in winter conditions.
