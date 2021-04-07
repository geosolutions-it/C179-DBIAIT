from app.scheduler.tasks.export_definitions.validations import IfValidation


class PostValidation:
    @staticmethod
    def validate_collett(val, excel_wb, logger):
        source_ato = [x.value for x in excel_wb[val['source'].capitalize()]['A']]
        target_ato = [x.value for x in excel_wb[val['target'].capitalize()]['B']]
        for pos, sa in enumerate(source_ato, 1):
            if sa in target_ato:
                index = target_ato.index(sa) + 1
                field = dict()
                field['102000'] = excel_wb[val['source'].capitalize()][f'N{pos}'].value
                field['68600'] = excel_wb[val['target'].capitalize()][f'I{index}'].value
                response = IfValidation(val['params']).validate(field)
                if not response:
                    logger.warning(val['warning'])

    @staticmethod
    def validate_fognat(val, excel_wb, logger):
        source_ato = [x.value for x in excel_wb[val['source'].capitalize()]['A']]
        target_ato = [x.value for x in excel_wb[val['target'].capitalize()]['B']]
        for pos, sa in enumerate(source_ato, 1):
            if sa in target_ato:
                index = target_ato.index(sa) + 1
                field = dict()
                field['101800'] = excel_wb[val['source'].capitalize()][f'N{pos}'].value
                field['101300'] = excel_wb[val['target'].capitalize()][f'Q{index}'].value
                response = IfValidation(val['params']).validate(field)
                if not response:
                    logger.warning(val['warning'])
