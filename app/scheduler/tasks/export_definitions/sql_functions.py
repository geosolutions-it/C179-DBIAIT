from pypika import CustomFunction

gb_x = CustomFunction("GB_X", ["v_geom"])
gb_y = CustomFunction("GB_Y", ["v_geom"])

SQL_FUNCTION_MAPPING = {"GB_X": gb_x, "GB_Y": gb_y}

SUPPORTED_SQL_FUNCTIONS = list(SQL_FUNCTION_MAPPING.keys())
