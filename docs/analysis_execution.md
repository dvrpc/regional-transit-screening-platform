# Executing the analysis

All elements of this codebase are accessible through a command-line interface (CLI)
as well as traditional script execution. You must have the `RTSP` environment
activated via `conda`.

## Create the database and import all necessary data

```bash
> RTSP db-setup-from-shp
> RTSP db-import-osm
> RTSP db-feature-engineering
> RTSP db-scrape-septa-report
```

---

## Average speeds

```bash
> RTSP speed-match-osm
> RTSP speed-analysis
```

## On Time Performance

- TODO

## Travel Time Index

- TODO

## Ridership

```bash
> RTSP ridership-match-osm-w-septa
> RTSP ridership-match-osm-w-njt
> RTSP ridership-analysis
```

---

To see the documentation and a list of all available commands, execute `RTSP --help`

```bash
> RTSP --help

Usage: RTSP [OPTIONS] COMMAND [ARGS]...

  RTSP allows command-line execution of the analysis.

Options:
  --help  Show this message and exit.

Commands:
  db-feature-engineering       Clean up source data for analysis
  db-import-from-daisy-db      Import data from the daisy 'GTFS' db
  db-import-osm                Import OpenStreetMap edges to the SQL db
  db-setup-from-shp            Create a local SQL db & import .shp and .csv...
  ridership-analysis           Calculate an average ridership value for OSM...
  ridership-match-osm-w-njt    Match SEPTA ridership segments with OSM...
  ridership-match-osm-w-septa  Match SEPTA ridership segments with OSM...
  speed-analysis               Calculate a weighted average speed for OSM...
  speed-match-osm              Match speed segments to OSM features
```
