# Data Inputs

This page identifies the datasets used within each analysis. The following dataset is used across all analyses:

`osm_edges`

- from OpenStreetMap, downloaded via `osmnx` on 5/5/2021
- Downloaded directly from the web to the PostgreSQL database. Data covers entire region.

## AVERAGE SPEED

`LinkSpeed_byLine.shp`

- GTFS / TIM 2.3 (2015 Base Year)
- Segments by line, with average speed and count of observations
  `linkspeedBylineNameCode.csv`
- Identifies the transit mode for each line name

## RIDERSHIP

`statsbyline_allgeom.shp`

- Survey of Transit Operators (2015-2017)
- Single-segment data for each transit line, used for NJTransit only

`passloads_segmentlevel_2020_07.shp`

- ? (recently recreated)
- SEPTA transit lines, broken down to the segment level |
