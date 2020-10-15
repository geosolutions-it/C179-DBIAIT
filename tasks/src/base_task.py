from itask import ITask


class BaseTask(ITask):

    def __init__(self, config=None):
        self.config = config

    def get_parameter(self, name):
        param = None
        if self.config is not None:
            if name in self.config:
                param = self.config[name]
        return param


