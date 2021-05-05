# step_00_helpers

This module contains functions that get used across
multiple other modules within this project.

## `cli.py`

All of the components of this analysis can be executed
on the command line using `RTSP`. For more details on the CLI,
please take a look at the [analysis execution documentation](../../documentation/analysis_execution.md).

## `database.py`

This module defines a class named `PostgreSQL` which handles
many of the typical database-level operations, such as connecting
to the database (and safely disconnecting!), importing data,
querying, and executing/commiting SQL code. This requires users to create a `.env`
file somewhere within the folder structure that defines the following
variables. An example of this file is shown below:

```
SQL_DB_NAME=my_database_name
DB_USER=my_username
DB_PW=my_password
```


## `interpolation.py`

All of the raw input datasets need to be matched up
with a consistent, regional-scale centerline base layer.

For this analysis, OpenStreetMap is being used as the
base network. The product of the "matchup" is a new
non-spatial table that records the `osmuuid` and the
associtaed `uid` from the spatial data table in question.