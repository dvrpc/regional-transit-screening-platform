# Executing the analysis

All elements of this codebase are accessible through a command-line interface (CLI)
as well as traditional script execution (i.e. ``python my_script.py``)

## Command-Line Interface

1) Create the database and import all necessary data

```bash
> RTSP db-setup-from-shp
> RTSP db-import-osm
```

2) Calculate average speeds


## Help 

To see the documentation and a list of all available commands, execute:

```bash
> RTSP --help
```

Output:

```bash
Usage: RTSP [OPTIONS] COMMAND [ARGS]...

  RTSP allows command-line execution of DVRPC regional transit analysis scripts.

Options:
  --help  Show this message and exit.

Commands:
  db-import-osm      Import OpenStreetMap edges to the SQL db
  db-setup-from-shp  Create a local SQL db & import .shp and .csv datasets
```