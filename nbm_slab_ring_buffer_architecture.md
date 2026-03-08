# NBM Slab Ring Buffer Architecture

## Purpose

This document specifies a simple, Python-friendly architecture for storing and serving a gridded forecast dataset derived from NBM GRIB2 input files.

The design is optimized for the following access pattern:

- Each API request supplies a spatial index `(i, j)`.
- The API must return a `(kvars, n_times)` array corresponding to all retained variables at that grid point across all stored forecast times.
- The implementation must avoid loading full slab arrays into RAM for each request.
- The implementation should avoid unnecessary I/O complexity, compression overhead, and container-format overhead.

This architecture deliberately avoids Zarr, HDF5 chunking, and compression. It relies instead on:

- one file per forecast-time slab,
- contiguous storage of the last axis (`kvars`),
- memory mapping for partial reads,
- and a fixed-size ring buffer of slab files.

---

## High-level design

Each forecast-time slab is stored as a separate on-disk array of shape:

```text
(idim, jdim, kvars)
```

where:

- `idim`, `jdim`: native spatial dimensions of the retained NBM grid,
- `kvars`: number of retained variables.

The ring buffer holds exactly `n_times` slab files, one for each forecast time currently retained.

Each slab file contains scaled 16-bit integer values (`int16`) in **C order**. This is important because a query of the form:

```text
arr[i, j, :]
```

then corresponds to a short contiguous block in memory and on disk.

The API process keeps the slab files memory-mapped. An API request for `(i, j)` reads the `kvars`-length vector from each slab and stacks the results into a `(kvars, n_times)` output array.

---

## Why this design

This design is intended to be the simplest architecture that satisfies the core requirements.

### Advantages

- No full-array RAM loading is required for serving point queries.
- No chunking metadata or chunk-cache tuning is needed.
- No decompression cost is incurred.
- Ring-buffer logic is simple and explicit.
- Parallel post-processing is straightforward because each incoming GRIB2 file is transformed into one independent slab file.
- The access pattern `arr[i, j, :]` is naturally efficient with contiguous storage.
- Using `int16` reduces file size and I/O volume substantially.

### Non-goals

This design does **not** attempt to optimize for:

- large neighborhood reads,
- spatial subsetting over broad regions,
- heavy analytical workloads across the full grid,
- cloud/object-store portability,
- or built-in compression.

If those become future requirements, a chunked format can be reconsidered later.

---

## Data model

### Slab definition

A slab represents one forecast-time slice of all retained variables over the full spatial grid.

Each slab has shape:

```python
(idim, jdim, kvars)
```

### Data type

The slab payload is stored as:

```python
np.int16
```

This assumes the retained variables do not require high precision and can be represented adequately using scale/offset conversion.

### Memory order

Store arrays in **C order**.

That ensures that:

```python
arr[i, j, :]
```

is contiguous, which is the critical access pattern for the API.

### Logical output shape

For an API request `(i, j)`, the returned logical result should have shape:

```python
(kvars, n_times)
```

where the second axis is ordered by logical forecast time, not necessarily by physical slot number in the ring buffer.

---

## On-disk format

### Recommended format

Use one `.npy` file per slab, opened with NumPy memory mapping.

Recommended writer/reader APIs:

- `numpy.lib.format.open_memmap` for writing,
- `numpy.load(..., mmap_mode='r')` or `open_memmap(..., mode='r')` for reading.

Reasons:

- `.npy` is simple and well supported in Python.
- dtype and shape are embedded in the file.
- memory mapping works naturally.
- no additional container layer is required.

### Alternative

A raw flat binary format with sidecar metadata is possible, but `.npy` is preferred unless there is a compelling reason to minimize format headers.

---

## Directory layout

A possible directory structure:

```text
store/
  metadata.json
  ring_state.json
  slab_000.npy
  slab_001.npy
  slab_002.npy
  ...
  slab_{n_times-1:03d}.npy
```

### `metadata.json`

Contains static dataset metadata, for example:

- `idim`
- `jdim`
- `kvars`
- `n_times`
- variable names
- units
- scale factors per variable
- offsets per variable
- fill value convention
- byte order if relevant
- description of spatial grid if needed by surrounding application

### `ring_state.json`

Contains dynamic ring-buffer state, for example:

- index of newest physical slot
- mapping from logical time index to physical slot
- valid times or forecast lead times associated with each logical time
- timestamp of last completed ingest
- version or generation counter for synchronization

The ring state must be updated atomically only after a new slab has been fully written and validated.

---

## Ring buffer semantics

### Basic idea

The ring buffer contains exactly `n_times` physical slab files.

At any moment:

- each physical slot contains one slab,
- the slots correspond to a logical time ordering,
- and when a new slab arrives, the oldest logical slab is overwritten.

### Recommended physical slot naming

Use fixed filenames for the physical slots:

```text
slab_000.npy
slab_001.npy
...
slab_{n_times-1}.npy
```

This avoids creating and deleting files on every update.

### Update rule

When a new GRIB2-derived slab arrives:

1. determine which physical slot currently corresponds to the oldest logical time,
2. overwrite that slot with the new slab,
3. update the ring-state mapping so that this slot now corresponds to the newest logical time,
4. atomically publish the new ring state.

### Important principle

Do **not** physically reorder files to reflect time order.

Instead:

- keep physical slots fixed,
- maintain logical ordering in metadata.

This keeps the update operation cheap and simple.

---

## Query path

### API input

The API receives:

- grid indices `(i, j)`

or anything that can be resolved upstream to those integer indices.

### Query steps

1. Validate that `(i, j)` is in bounds.
2. Read the current `ring_state.json`.
3. Resolve logical time order to physical slab order.
4. For each logical time:
   - access the corresponding memory-mapped slab,
   - read `slab[i, j, :]`, which yields an `int16` vector of length `kvars`.
5. Stack the vectors into an `int16` array of shape `(kvars, n_times)`.
6. Convert to floating-point if the API contract requires physical values rather than packed values.
7. Apply scale/offset per variable.
8. Return the result along with any needed time metadata.

### Important implementation note

The service should not open and close all slab files for every request.

Instead, the API process should keep the slab files memory-mapped and reuse those mappings across requests. Only the slab that gets replaced during ring advancement should need to be reopened or remapped.

---

## Scale and offset handling

### Recommended approach

Use fixed per-variable packing metadata shared across all slabs.

For each retained variable `k`, define:

- `scale_factor[k]`
- `add_offset[k]`
- optional `fill_value[k]`

Then unpack using the usual relation:

```python
physical_value = packed_value * scale_factor[k] + add_offset[k]
```

### Why fixed metadata is preferable

If packing parameters are fixed by variable rather than varying by slab:

- metadata is simpler,
- files are easier to interpret,
- API implementation is cleaner,
- and downstream behavior is more predictable.

If some variables genuinely require slab-dependent packing, that can be added later, but the initial implementation should prefer fixed packing.

---

## Missing values

The implementation should define a clear missing-value convention for packed `int16` data.

Recommended options:

- reserve a sentinel such as `-32768` for missing,
- record it in metadata,
- and convert it to `NaN` after unpacking if the API returns floating-point values.

Any ingest pipeline must ensure the sentinel does not collide with valid packed data.

---

## Ingest pipeline

### Input

Each ingest cycle begins with one incoming NBM GRIB2 file.

### Processing stages

1. Read the GRIB2 file.
2. Extract only the retained variables required by the application.
3. Reproject or subset only if needed by the broader system design.
4. Convert each retained variable to the packed `int16` representation.
5. Assemble a slab array of shape `(idim, jdim, kvars)`.
6. Write the slab into the designated physical ring-buffer slot.
7. Flush and validate the file.
8. Atomically update ring-state metadata.

### Parallelism

Up to 16 workers may be used for the post-processing stage before the final slab is written.

A reasonable pattern is:

- divide the retained variables and/or spatial work across workers,
- have workers compute packed outputs into shared or temporary arrays,
- assemble the final slab,
- then perform one coordinated write into the target slot.

Because each ingest cycle writes only one slab file, synchronization is much simpler than in a chunked multi-writer store.

---

## Concurrency model

### Readers

The API service is predominantly read-oriented.

It should treat the slab files and ring state as effectively read-only during query execution.

### Writer

The ingest pipeline is the only writer.

It updates one slab slot and then publishes a new ring state.

### Publication rule

A newly written slab must not become visible to readers until:

- the slab file write is complete,
- the file is flushed,
- and the ring-state update has been atomically committed.

### Recommended strategy

The implementation should ensure one of the following:

- write slab to a temporary file and atomically rename into the slot, or
- overwrite the slot safely and only update ring-state metadata once the write is confirmed complete.

For ring-state publication, prefer an atomic replace of `ring_state.json` rather than in-place editing.

### Reader consistency

A query should either see:

- the old complete ring state,
- or the new complete ring state,

but never a partially updated state.

---

## Performance expectations

### Per-slab query cost

For one slab, a query reads only:

```text
kvars * 2 bytes
```

of payload for the packed `int16` values, plus normal page/cache overhead.

For example, if `kvars = 15`, the raw payload per slab is only 30 bytes.

### Per-request cost

For `n_times` slabs, the query reads:

```text
n_times * kvars * 2 bytes
```

of payload, plus filesystem and page-cache overhead.

This means the raw data volume per API request is very small. In practice, the dominant costs are more likely to be:

- Python overhead,
- metadata lookup,
- memory-map/page behavior,
- and API serialization.

### Consequence

This is a good fit for a memory-mapped slab design.

---

## Caching strategy

### Strong recommendation

Maintain persistent memory-mapped handles for the currently active slab files.

Do not repeatedly open and close slab files on every API request unless request rates are extremely low.

### Suggested approach

At service startup:

- load metadata,
- open/memory-map all slab files,
- load ring-state metadata,
- build the current logical-to-physical mapping.

On each ingest update:

- detect the newly replaced slot,
- refresh only that memmap if necessary,
- reload ring-state metadata.

This minimizes overhead and keeps the hot path simple.

---

## Suggested Python components

The implementation agent may use classes roughly along these lines.

### `SlabMetadata`

Responsibilities:

- hold static metadata,
- validate dimensions,
- hold variable names, scales, offsets, and fill values,
- serialize/deserialize `metadata.json`.

### `RingState`

Responsibilities:

- hold logical-to-physical slot mapping,
- track newest slot,
- hold valid-time metadata,
- serialize/deserialize `ring_state.json`,
- provide helper methods for logical ordering.

### `SlabWriter`

Responsibilities:

- create or overwrite a target slab slot,
- write packed `int16` slab arrays,
- validate dtype/shape/order,
- flush safely,
- coordinate atomic publication.

### `SlabRingBuffer`

Responsibilities:

- manage fixed slot filenames,
- determine which physical slot to overwrite next,
- coordinate with `RingState`,
- expose methods for ingest/update.

### `NBMStore`

Responsibilities:

- open and cache memory-mapped slab files,
- answer point queries `(i, j)`,
- assemble `(kvars, n_times)` results,
- unpack values if requested,
- return associated time metadata.

---

## Suggested API behavior

A possible internal method signature:

```python
def get_point_timeseries(i: int, j: int, unpack: bool = True) -> tuple[np.ndarray, list]:
    ...
```

Expected return:

- array of shape `(kvars, n_times)`
- companion time metadata in logical order

The public API layer can then serialize this into JSON, MessagePack, NumPy binary, or another transport format as appropriate.

---

## Validation requirements

The implementation should include checks for:

- correct slab shape `(idim, jdim, kvars)`
- `int16` dtype
- C-order contiguity before writing
- valid `(i, j)` query bounds
- consistent `kvars` length across metadata and files
- consistent number of slab slots
- valid ring-state mapping with no duplicates or missing slots
- correct handling of missing-value sentinel
- correct scale/offset unpacking

---

## Operational cautions

### Avoid per-request remapping

Do not recreate memmaps for every query.

### Avoid file churn

Do not create and delete new slab filenames each cycle if fixed physical slot names are sufficient.

### Avoid implicit time ordering from filenames

Time ordering should come from ring-state metadata, not from alphabetical filename order.

### Avoid slab-dependent packing unless necessary

Use fixed per-variable packing initially.

### Avoid partial publication

Never update ring-state metadata before the slab write is complete.

---

## Future extensions

Possible future enhancements, if requirements evolve:

- optional neighborhood queries around `(i, j)`
- optional variable subsetting
- optional return of packed values vs unpacked floats
- per-variable files if later benchmarks justify them
- migration to a chunked store if future workloads become spatially broader
- precomputed index translation from lat/lon to `(i, j)`
- shared-memory or vectorized batching for many-point queries

These are out of scope for the initial implementation.

---

## Recommended implementation baseline

The initial implementation should assume:

- one `.npy` file per forecast-time slab,
- `int16` packed values,
- shape `(idim, jdim, kvars)`,
- C-order layout,
- fixed ring-buffer slot files,
- static dataset metadata in `metadata.json`,
- dynamic logical/physical time mapping in `ring_state.json`,
- persistent memmaps in the API process,
- atomic publication of each ingest update.

This should provide a simple, robust foundation with low RAM usage and very low per-request data movement for the target query pattern.
