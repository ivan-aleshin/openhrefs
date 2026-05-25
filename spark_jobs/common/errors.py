"""Exception hierarchy for openhrefs.

All recoverable, domain-specific failures derive from ``OpenhrefsError`` so
entrypoints can catch the family. Unrecoverable failures are left to bubble up.
"""


class OpenhrefsError(Exception):
    """Base class for all openhrefs domain errors."""


class ConfigError(OpenhrefsError):
    """Configuration is missing, unparseable, or references an unknown value."""


class ScopeFilterError(OpenhrefsError):
    """Scope grammar is invalid or cannot be applied."""


class SparkJobError(OpenhrefsError):
    """A Spark stage failed in a way the job can describe."""


class DataSourceError(OpenhrefsError):
    """An input dataset is missing, malformed, or has an unexpected schema."""
