# New South Wales Valuer General Data

There's 2 schemas here:

- `nsw_vg`: Which is the chema for land values data
- `nsw_vg_raw`: Which is the schema for raw data read
  from data published by the valuer general.

## What is stored in `nsw_vg_raw`

This is for storing the raw data with minimal processing
from the following sources:

- [**Bulk Property Sales Information**][bulk-psi]
- [**Bulk Land Value**][bulk-lv]

[bulk-psi]: https://valuation.property.nsw.gov.au/embed/propertySalesInformation
[bulk-lv]: https://www.valuergeneral.nsw.gov.au/land_value_summaries/lv.php

The reason for storing the data here and not just immediately
in the tables that are derived from here, is mostly a matter
of practicallity and flexibility.

- This allows the logic for ingesting the data to focus on getting
  the data into the database with minimial complexity.
- And any complexity in deriving more structured data can be kept
  in SQL and run in the database.

