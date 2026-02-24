class QDArchiveError(Exception):
    """Base exception for QDArchive seeding."""


class ConfigError(QDArchiveError):
    """Raised when configuration is invalid."""


class RegistryError(QDArchiveError):
    """Raised when registry operations fail."""


class ValidationError(QDArchiveError):
    """Raised when validation fails."""
