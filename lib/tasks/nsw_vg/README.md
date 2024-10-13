# Ingesting NSW VG

Here's an example of how you can ingest the NSW VG data.

```
python -m lib.tasks.nsw_vg.ingest \
    --instance 1 --main-debug \
    --load-land-values \
    --load-property-sales \
    --ps-workers 6 \
    --ps-worker-db-pool-size 4 --ps-worker-db-batch-size 1000
```

The above may go out of sync, you can just use the `--help` flag
to get information on the different options. For the most part
anything starting with `--ps-*` just configure different the amount
of computer resources are used to ingest the property sales.

For the most part they don't really matter unless you want to speed
things up or reduce the amount of resources allocated. But you will
need to specify the number of workers with `--ps-workers`.

