class FocusValidationError(Exception):
    pass


class FocusNotImplementedError(FocusValidationError):
    def __init__(self, msg=None):
        super().__init__(msg)


class UnsupportedVersion(FocusValidationError):
    pass


class FailedDownloadError(FocusValidationError):
    pass


class InvalidRuleException(ValueError):
    """Raised when a rule's requirement/spec is invalid or incomplete."""

    pass
