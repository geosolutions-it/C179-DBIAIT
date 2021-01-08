from pypika import CustomFunction

is_null = CustomFunction("IS_NULL", ["v_value"])
gb_x = CustomFunction("GB_X", ["v_geom"])
gb_y = CustomFunction("GB_Y", ["v_geom"])
st_x = CustomFunction("ST_X", ["v_geom"])
st_y = CustomFunction("ST_Y", ["v_geom"])
st_round_x = CustomFunction("ST_ROUND_X", ["v_geom"])
st_round_y = CustomFunction("ST_ROUND_Y", ["v_geom"])

st_transform_4326 = CustomFunction("ST_TRANSFORM_4326", ["v_geom"])
to_bit = CustomFunction("TO_BIT", ["v_value"])
from_float_to_int = CustomFunction("FROM_FLOAT_TO_INT", ["v_value"])

SQL_FUNCTION_MAPPING = {
    "IS_NULL": is_null,
    "GB_X": gb_x, "GB_Y": gb_y,
    "ST_X": st_x, "ST_Y": st_y,
    "ST_ROUND_X": st_round_x,
    "ST_ROUND_Y": st_round_y,
    "ST_TRANSFORM_4326": st_transform_4326,
    "TO_BIT": to_bit,
    "FROM_FLOAT_TO_INT": from_float_to_int
}

SUPPORTED_SQL_FUNCTIONS = list(SQL_FUNCTION_MAPPING.keys())

st_intersects = CustomFunction("ST_INTERSECTS", ["geomA", "geomB"])
st_contains = CustomFunction("ST_CONTAINS", ["geomA", "geomB"])

SQL_SPATIAL_JOIN_MAPPING = {"ST_INTERSECTS": st_intersects, "ST_CONTAINS": st_contains}
