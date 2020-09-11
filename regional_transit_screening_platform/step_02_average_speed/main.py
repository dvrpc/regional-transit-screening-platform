from regional_transit_screening_platform import db


link_table = "linkspeed_byline"
speed_col = "avgspeed"


# Make a table that has all unique geometries
unique_links = f"""
    SELECT fromnodeno, tonodeno, geom
    FROM {link_table}
    GROUP BY fromnodeno, tonodeno, geom
"""
db.make_geotable_from_query(
    query=unique_links,
    new_table_name=f"{link_table}_unique",
    geom_type="LINESTRING",
    epsg=26918
)

query_link_speeds = f"""
    SELECT *
    FROM {link_table}
    WHERE {speed_col} IS NOT NULL
        AND
          {speed_col} > 0
"""

print(query_link_speeds)

gdf = db.query_as_geo_df(query_link_speeds)
