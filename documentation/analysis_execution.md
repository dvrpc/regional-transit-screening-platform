# Executing the analysis

All elements of this codebase are accessible through a command-line interface (CLI)
as well as traditional script execution. You must have the ``RTSP`` environment
activated via ``conda``.

## via CLI

1) Create the database and import all necessary data

```bash
> RTSP db-setup-from-shp
> RTSP db-import-osm
```

2) Average speeds
```bash
> RTSP speed-match-osm
> RTSP speed-analysis
```

3) ...

4) ...

5) Ridership
```bash
> RTSP ridership-match-osm-w-septa
> RTSP ridership-match-osm-w-njt
> RTSP ridership-analysis
```

## via traditional script

```bash
> ipython regional_transit_screening_platform/step_01_import_data/main.py
> ipython regional_transit_screening_platform/step_02_average_speed/main.py
>
>
> ipython regional_transit_screening_platform/step_05_ridership/main.py
```

## Help 

To see the documentation and a list of all available commands, execute:

```bash
> RTSP --help
```

Output:

```bash
Usage: RTSP [OPTIONS] COMMAND [ARGS]...

  RTSP allows command-line execution of the analysis.

Options:
  --help  Show this message and exit.

Commands:
  db-import-osm                Import OpenStreetMap edges to the SQL db
  db-setup-from-shp            Create a local SQL db & import .shp and .csv...
  ridership-analysis           Calculate an average ridership value for OSM...
  ridership-match-osm-w-septa  Match ridership segments with OSM features
  speed-analysis               Calculate a weighted average speed for OSM...
  speed-match-osm              Match speed segments to OSM features
```