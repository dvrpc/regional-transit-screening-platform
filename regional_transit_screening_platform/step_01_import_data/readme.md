# step_01_import_data

This module is responsibe for the import
of all shapefile and CSV datasets
into a new PostgreSQL database.

## Assumptions

- All commands assume you're using a terminal with the proper `conda` environment activated.
- You'll need some data directly from the `GTFS` database on `daisy`

### Create the database and import shapefiles / CSVs

```bash
> RTSP db-setup-from-shp
```

### Download & import data from OpenStreetMap

```bash
> RTSP db-import-osm
```

You can also execute the code by running the script itself:

```bash
> python regional_transit_screening_platform/step_01_import_data/main.py
```
