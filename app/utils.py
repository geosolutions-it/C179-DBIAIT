from typing import Dict
from string import Template

from copy import deepcopy


class TemplateWithDefaults(Template):

    def __init__(self, template, defaults: Dict = None):
        super().__init__(template)
        self.defaults = defaults or {}

    def mapping(self, mapping):
        default_mapping = deepcopy(self.defaults)
        default_mapping.update(mapping)
        return default_mapping

    def substitute(self, mapping=None, **kws):
        return super().substitute(self.mapping(mapping or {}), **kws)

    def safe_substitute(self, mapping=None, **kws):
        return super().safe_substitute(self.mapping(mapping or {}), **kws)
