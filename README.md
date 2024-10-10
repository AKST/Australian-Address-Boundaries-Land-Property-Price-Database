# Read me

> NOTE THE MAIN BRANCH IS NOT STABLE ATM

After starting docker, and installing the dependencies, go to the `data-ingestion` notebook.

## Tests

```
./scripts/check_tests.sh
```

## Data used

- [Abs Shapefiles][Abs boundaries]
- [Geoscape Geocoded National Address File][gnaf]
- [NSW Valuer General land prices][nswvglv]
- [NSW Valuer General property sales][nswvgps]

[Abs boundaries]: https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files
[gnaf]: https://data.gov.au/data/dataset/geocoded-national-address-file-g-naf
[nswvglv]: https://www.valuergeneral.nsw.gov.au/land_value_summaries/lv.php
[nswvgps]: https://valuation.property.nsw.gov.au/embed/propertySalesInformation

### Disclaimer

Please respect the respective terms of use of the different dataset provided.

## General Structure

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

## Questions

### Why are there 2 data ingestion notebooks?
I am too lazy to use version control. In fact the main reason why it's in
version control is mostly to share what I got with others less so version
control, I literally just go `git add . && git commit -m 'idk stuff'` once a
week. Actually prior to making this a repo this was a directory in a larger
project with hundreds of other small data projects.

### What are the note books starting with `pg_`
Those are mostly for experimenting, in particular experimenting with data to
find how to best ingest it into the database, or figuring out how the general
data structure and how to use that data.

### What's the implication of the license
It's a standard MIT license, basically I don't care what you do with this code,
modify it as you please, use it to make money for yourself, just don't sue me
if you misuse and it causes you some kind of harm.

I will have zero claim over any work done with the aid of this project, and
I won't be entitled to any shout out or thanks for any assistance this project
may offer the user in any of their endevours.

Basically I'm sharing this to make increase access to this kind of information,
as I know I personally found it hard to process half the datasets this database
consumed.

Why add? Just basic projection from liability.
