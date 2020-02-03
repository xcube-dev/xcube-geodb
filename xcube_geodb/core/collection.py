from typing import Dict, Sequence


class Collection:
    def __init__(self, name: str, props: Dict):
        self._config = {name: props}
        self._name = name

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        self._config = value

    @property
    def name(self):
        return self._name

    def add_props(self, properties: Dict):
        self._config[self._name].update(properties)
        return self

    def delete_props(self, props: Sequence):
        for prop in props:
            del self._config[self._name][prop]

    def _repr_pretty_(self, p, cycle):
        import pprint
        if cycle:
            p.text(pprint.pformat(self._config))
        else:
            p.text(pprint.pformat(self._config))

