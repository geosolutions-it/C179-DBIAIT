import ast
import os


def export_vars(request):
    data = dict()
    data['URL_PATH_PREFIX'] = os.getenv("URL_PATH_PREFIX", "")
    data['FREEZE_FEATURE_TOGGLE'] = ast.literal_eval(os.getenv("FREEZE_FEATURE_TOGGLE", "False"))
    return data
