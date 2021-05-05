# RTSP Methodology

For each analysis module, the overall methodology consists of:

1. associating input data segments to the matching geometries in OpenStreetMap
2. calculating the specific metric(s) for each associated OSM segment

## Average Speed

For each OSM segment, calculate a weighted average speed value with SQL:

```
select
    sum(cnt * speed) / sum(cnt) as avgspeed,
    count(speed) as num_obs
from {speed_table}
where uid in (select distinct speed_uid
              from osm_speed_matchup m
              where m.osmid = '{osmid}')
```

Note: This is run against a copy of `linkspeed_byline` with dropped null/zeros and filtered to surface transit only |

## Ridership

Use the line-level data for NJTransit and segment-level data for SEPTA.

## On Time Performance

- TODO

## Travel Time Index

- TODO
