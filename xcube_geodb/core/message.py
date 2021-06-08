class Message:
    def __init__(self, message: str):
        self._message = message

    @property
    def message(self):
        return self._message

    def _repr_pretty_(self, p, cycle):
        p.text(self._message)

    def __repr__(self):
        return f"<h1>{self._message}</h1>"
