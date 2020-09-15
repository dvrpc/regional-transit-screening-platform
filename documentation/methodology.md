# RTSP Methodology

For each analysis module, the overall methodology consists of:
1) associating input data segments to the matching geometries in OpenStreetMap
2) calculating the specific metric(s) for each associated OSM segment

## Database Setup

Import all of the required datasets into a PostgreSQL database.

| Analysis | Input Dataset | Source | Description |
| ---      | ---           | ---    | ---         |
| Average Speed | `LinkSpeed_byLine.shp` | GTFS / TIM 2.3 (2015 Base Year) | Segments by line, with average speed and count of observations |
| Average Speed | `linkspeedBylineNameCode.csv` | ? | Identifies the transit mode for each line name |
| Ridership | `statsbyline_allgeom.shp` | Survey of Transit Operators (2015-2017) | Single-segment data for each transit line, used for NJTransit only |
| Ridership | `passloads_segmentlevel_2020_07.shp` | ? (recently recreated) | SEPTA transit lines, broken down to the segment level |

---

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

| Variable | Value | Notes |
| ---      | ---   | ---   |
| `{osmid}` | e.g. `424986597` or `{404615586,12113187}` | This is a `TEXT` datatype |
| `{speed_table}` | `linkspeed_byline_surface` | This is a copy of `linkspeed_byline` with dropped null/zeros and filtered to surface transit only |

---

## Ridership

Use the line-level data for NJTransit and segment-level data for SEPTA.


## Backlog

- ### On Time Performance

- ### Travel Time Index
