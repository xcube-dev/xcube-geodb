from typing import Dict


class Collection:
    def __init__(self, config: Dict):
        self._config = config

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        self._config = value

    def add(self, collection: str, properties: Dict):
        self._config[collection] = properties
        return self

    def _repr_pretty_(self, p, cycle):
        import pprint
        if cycle:
            p.text(pprint.pformat(self._config))
        else:
            p.text(pprint.pformat(self._config))

