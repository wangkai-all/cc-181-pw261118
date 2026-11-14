"""Exception types used by the engine and mapped onto HTTP status codes."""


class PosdelError(Exception):
    """Base class for every engine error."""

    status = 400
    code = "posdel_error"

    def __init__(self, message: str, **details):
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict:
        return {"error": self.code, "message": self.message, "details": self.details}


class ValidationError(PosdelError):
    status = 422
    code = "validation_error"


class NotFound(PosdelError):
    status = 404
    code = "not_found"


class Conflict(PosdelError):
    """Concurrent writers touched the same underlying data.

    Raised when a merge/append was prepared against a snapshot that has since
    changed in a way that invalidates the prepared work (for example the rows
    being rewritten were deleted meanwhile).
    """

    status = 409
    code = "conflict"


class RecoveryRequired(PosdelError):
    """An interrupted commit left state that must be repaired first."""

    status = 409
    code = "recovery_required"


class CorruptState(PosdelError):
    """On-disk state failed an integrity check."""

    status = 500
    code = "corrupt_state"
