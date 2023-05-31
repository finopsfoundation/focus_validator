class FocusValidationError(Exception):
    pass


class FocusNotImplementedError(FocusValidationError):
    def __init__(self, msg=None):
        super().__init__(msg)


class UnsupportedVersion(FocusValidationError):
    pass
