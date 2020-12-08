from pypika import CustomFunction

is_null = CustomFunction("IS_NULL", ["v_value"])
gb_x = CustomFunction("GB_X", ["v_geom"])
gb_y = CustomFunction("GB_Y", ["v_geom"])
st_transform_4326 = CustomFunction("ST_TRANSFORM_4326", ["v_geom"])
to_bit = CustomFunction("TO_BIT", ["v_value"])

SQL_FUNCTION_MAPPING = {
    "IS_NULL": is_null,
    "GB_X": gb_x, "GB_Y": gb_y,
    "ST_TRANSFORM_4326": st_transform_4326,
    "TO_BIT": to_bit
}

SUPPORTED_SQL_FUNCTIONS = list(SQL_FUNCTION_MAPPING.keys())

st_intersects = CustomFunction("ST_INTERSECTS", ["geomA", "geomB"])
st_contains = CustomFunction("ST_CONTAINS", ["geomA", "geomB"])

SQL_SPATIAL_JOIN_MAPPING = {"ST_INTERSECTS": st_intersects, "ST_CONTAINS": st_contains}
