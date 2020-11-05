import json
from enum import Enum
import schema
import logging
import pypika
from pypika import Query, Table, Field

from django.conf import settings

logger = logging.getLogger(__name__)


class JoinType(Enum):
    inner = ""
    left = "LEFT"
    right = "RIGHT"
    outer = "FULL OUTER"
    spatial = 'SPATIAL'


JOIN_TYPES = ['INNER', 'LEFT', 'RIGHT', 'OUTER', 'SPATIAL']
SQL_COMPARISON_OPERATORS = ['=', '>', '<', '>=', '<=', '<>']
SUPPORTED_TRANSFORMATIONS = ['EMPTY', 'CONST', 'DIRECT', 'DOMAIN', 'LSTRIP', 'EXPR', 'IF', 'CASE']
SUPPORTED_VALIDATIONS = ['IF']

export_config_schema = schema.Schema(
    [
        {
            'sheet': schema.And(str, len),
            'skip': bool,
            'pre_process': schema.Optional(str),
            'sources': [
                schema.Or(
                    {
                        'raw': schema.And(str, len),
                    },
                    {
                        'table': {
                            'name': schema.And(str, len),
                            schema.Optional('alias'): schema.And(str, len)
                        },
                        'fields': [
                            {
                                'name': schema.And(str, len),
                                schema.Optional('alias'): schema.And(str, len)
                            }
                        ],
                        schema.Optional('join'): [
                            {
                                'type': schema.And(str, lambda t: t.upper() in JOIN_TYPES),
                                'table': {
                                    'name': schema.And(str, len),
                                    schema.Optional('alias'): schema.And(str, len)
                                },
                                'on': schema.And(
                                    [
                                        schema.And(str, len)
                                    ],
                                    lambda l: len(l) == 2
                                ),
                                'cond': schema.And(str, lambda v: v in SQL_COMPARISON_OPERATORS)
                            }
                        ],
                        schema.Optional('filter'): schema.And(str, len),
                        schema.Optional('group_by'): [
                            schema.And(str, len)
                        ],
                        schema.Optional('having'): [
                            schema.And(str, len)
                        ],
                    },
                    only_one=True)
            ],
            'columns': [
                {
                    'id': schema.And(schema.Or(str, int), lambda v: len(str(v)) > 0),
                    'field': schema.And(str, len),
                    'transformation': {
                        'func': schema.And(str, lambda v: v.upper() in SUPPORTED_TRANSFORMATIONS),
                        schema.Optional('params'): {str: object}
                    },
                    schema.Optional('validations'): [
                        {
                            'func': schema.And(str, lambda v: v.upper() in SUPPORTED_VALIDATIONS),
                            schema.Optional('params'): {str: object},
                            schema.Optional('warning'): str,
                        }
                    ]
                }
            ]
        },
    ]
)


class ExportConfig:

    def __init__(self):
        self.config = []

        with open(settings.EXPORT_CONF_FILE, 'r') as ecf:
            config = json.load(ecf)

        export_config_schema.validate(config)

        for sheet in config:

            sources = sheet.pop('sources')

            sql_sources = []
            for source in sources:

                if source.get('raw', None) is not None:
                    sql_sources.append(source['raw'])
                else:
                    # sql query builder
                    table_config = None
                    if table_config is None:
                        logger.info(f'Export config does not define the "table" key in "sources" for {sheet["sheet"]}.')
                        continue

                    table = Table(source['table']).as_(source.get('alias', source['table']))
                    fields = [
                        Field(field['name'].split('.')[0], table=Table(field['name'].split('.')[1])).as_(field.get('alias', field['name'])) for field in source['fields']
                    ]
                    query = Query.from_(table)

                    for join_table_config in source.get('join', []):
                        query.join(join_table_config, pypika.JoinType)

            sheet.update({'sql_sources': sql_sources})

            self.config.append(sheet)
