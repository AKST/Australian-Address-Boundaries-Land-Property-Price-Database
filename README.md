# Read me

After starting docker, and installing the dependencies, go to the `data-ingestion` notebook.

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
I won't be entitled to any shout out of thanks for any assistance this project
offer the user of it in any way.

Basically I'm sharing this to make increase access to this kind of information,
as I know I personally found it hard to process half the datasets this database
consumed.

## General Structure

- `lib` this is where all reusable code between notebooks are
- `web-out` this is where all the content is downloaded too
- `zip-out` this is where all zips are zipped into.
