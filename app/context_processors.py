import os


def export_vars(request):
    data = dict()
    data['URL_PATH_PREFIX'] = os.getenv("URL_PATH_PREFIX", "")
    return data
