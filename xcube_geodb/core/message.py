class Message:
    def __init__(self, message: str):
        self._message = message

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text(self._message)
        else:
            p.text(self._message)

    def __repr__(self):
        return f"<h1>{self._message}</h1>"
