from abc import ABCMeta, abstractmethod


class ITask:
    __metaclass__ = ABCMeta

    @classmethod
    def version(self): return "1.0"

    @abstractmethod
    def run(self): raise NotImplementedError

    @abstractmethod
    def progress(self, value): raise NotImplementedError

    @abstractmethod
    def done(self): raise NotImplementedError
