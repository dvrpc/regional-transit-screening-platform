# step_02_average_speed

This module uses the average speed data source
to create a 0 - 100 scale that combines all overlapping
segments, normalized by the number of trips along each segment.

## Usage

```bash
> RTSP speed-match-osm
> RTSP speed-analysis
```

The first command will compare OSM geometries to the geometries contained in the speed data source. It creates an interim table that maps `uuid` values from the OSM data to associated `uid` values from the source data.

The second command leverages the table from the first command to calculate weighted average speed values for the OSM segments that matched. In the source data each feature contains a count and average speed (`cnt` and `avgspeed`, respectively).

## TODO:

- [] Transform MPH values to 0-100 scale

## QAQC

- [] Investigate speed features with values over 75 MPH
- [] Investigate speed results with 50+ MPH on city grid
