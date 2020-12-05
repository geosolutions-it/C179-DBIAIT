from pypika import CustomFunction

gb_x = CustomFunction("GB_X", ["v_geom"])
gb_y = CustomFunction("GB_Y", ["v_geom"])
st_transform_4326 = CustomFunction("ST_TRANSFORM_4326", ["v_geom"])

SQL_FUNCTION_MAPPING = {"GB_X": gb_x, "GB_Y": gb_y, "ST_TRANSFORM_4326": st_transform_4326}

SUPPORTED_SQL_FUNCTIONS = list(SQL_FUNCTION_MAPPING.keys())

st_intersects = CustomFunction("ST_INTERSECTS", ["geomA", "geomB"])
st_contains = CustomFunction("ST_CONTAINS", ["geomA", "geomB"])

SQL_SPATIAL_JOIN_MAPPING = {"ST_INTERSECTS": st_intersects, "ST_CONTAINS": st_contains}
