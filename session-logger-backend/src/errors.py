"""
TODO: Module docstring
"""

class CustFlaskException(Exception):
    """
    TODO: class docstring
    """

    def __init__(self, message: str, status_code=None, payload=None) -> None:
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        if payload is not None:
            self.payload = payload

    def to_dict(self):
        """
        TODO: method docstring
        """
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv
