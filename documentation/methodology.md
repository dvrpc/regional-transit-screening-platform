# RTSP Methodology

For each module, the overall methodology consists of:
1) associating input data segments to the matching geometries in OpenStreetMap
2) calculating the specific metric(s) for each associated OSM segment

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

| Input Dataset | Source | Notes |
| ---      | ---    | ---   |
| `LinkSpeed_byLine.shp` | GTFS / TIM 2.3 (2015 Base Year) | Must join the CSV to filter by mode |
| `linkspeedBylineNameCode.csv` | ? | Identifies the transit mode by line name |

___

## Ridership

Use the line-level data for NJTransit and segment-level data for SEPTA.

| Input Dataset | Source | Notes |
| ---      | ---    | ---   |
| `statsbyline_allgeom.shp` | Survey of Transit Operators (2015-2017) | Used for NJTransit only |
| `passloads_segmentlevel_2020_07.shp` | ? (recently recreated) | SEPTA data only |

## Backlog

- ### On Time Performance

- ### Travel Time Index
