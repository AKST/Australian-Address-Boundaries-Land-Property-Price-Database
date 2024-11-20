# Read me

Significantly lower the barrier to working with NSW
geographic, planning, land and property data.

## Getting Started

1. Install and start [docker](https://www.docker.com).
2. Create your [virtual environment](https://docs.python.org/3/library/venv.html) and then install
3. Install the dependencies
   ```
   # I'm sorry I know there are more modern ways of
   # handling dependencies in python but I haven't
   # gotten around to learning them yet.
   pip install -r requirements.txt
   ```
4. Run the following within the virtual env
   ```
   python -m lib.tasks.ingest --instance 1
   ```

It may take an hour or so, but you'll see logs as you go.

### Connecting to the database

You will find the [credentials to local instance of the database here][pg_creds].

[pg_creds]: https://github.com/AKST/Australian-Address-Boundaries-Land-Property-Price-Database/blob/main/lib/service/database/defaults.py#L13

## Data Sources

- [Abs Shapefiles][Abs boundaries]
- [Geoscape Geocoded National Address File][gnaf]
- [NSW Valuer General land prices][nswvglv]
- [NSW Valuer General property sales][nswvgps]
- [NSW Spatial Services GIS data][nswssgis]

[Abs boundaries]: https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files
[gnaf]: https://data.gov.au/data/dataset/geocoded-national-address-file-g-naf
[nswvglv]: https://www.valuergeneral.nsw.gov.au/land_value_summaries/lv.php
[nswvgps]: https://valuation.property.nsw.gov.au/embed/propertySalesInformation
[nswssgis]: https://portal.spatial.nsw.gov.au/server/rest/services

### Disclaimer

Please respect the respective terms of use of the different dataset provided.

Especially once you either publish visualisations using the data or start
using it for business reasons, you should understand what the terms of use
are.

## Contributing

### General Structure

- `lib` this is where all reusable code between notebooks are
- `sql` this may suprise you but this is where I store SQL.
    - This is mostly loaded in the data ingestion notebook.
- `_out_web` this is where all the content is downloaded too
- `_out_zip` this is where all zips are zipped into.
- `_out_state` this is application state.
- `_out_cache` this is similar to `_web_out` in that it
  mostly exists to avoid repeating API calls. This is just
  significantly less human readable (files with UUID names).
  - The main usecase is minimizing API calls to GIS servers

### Tests

```
./scripts/check_tests.sh
```

### Types

```
./scripts/check_types.sh
```

## Questions

### What are the note books starting with `pg_`

Those are mostly for experimenting, in particular experimenting with data to
find how to best ingest it into the database, or figuring out how the general
data structure and how to use that data.

