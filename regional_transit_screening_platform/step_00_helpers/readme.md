# step_00_helpers

This module contains functions that get used across
multiple other modules within this project.

## `interpolation.py`

All of the raw input datasets need to be matched up
with a consistent, regional-scale centerline base layer.

For this analysis, OpenStreetMap is being used as the
base network. The product of the "matchup" is a new
non-spatial table that records the `osmuuid` and the
associtaed `uid` from the spatial data table in question.