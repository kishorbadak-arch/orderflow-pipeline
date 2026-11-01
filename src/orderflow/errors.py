"""Exception taxonomy.

The important split is *retryable* (infrastructure hiccups) versus *permanent*
(bad data, bad config, failed quality gate). The orchestrator only retries the former.

Messages of these exceptions must never contain row contents or secrets.
"""


class PipelineError(Exception):
    retryable = False


class TransientError(PipelineError):
    """Temporary infrastructure problem (I/O hiccup, locked DB). Safe to retry."""

    retryable = True


class TaskTimeout(PipelineError):
    """A task exceeded its time budget. Treated as transient (bounded retries)."""

    retryable = True


class BatchError(PipelineError):
    """The whole input batch is unusable. Permanent: retrying the same input cannot help."""


class MissingSourceError(BatchError):
    pass


class UnsupportedFileTypeError(BatchError):
    pass


class MalformedFileError(BatchError):
    pass


class BatchSchemaError(BatchError):
    pass


class EmptyInputError(BatchError):
    pass


class QualityGateError(BatchError):
    pass


class StaleSnapshotError(BatchError):
    pass


class PublishError(PipelineError):
    """Publication failed. The previously published output is left intact."""


class ConfigError(Exception):
    """Invalid configuration (CLI exit code 2)."""


class LockHeldError(Exception):
    """Another run is in progress (CLI exit code 3)."""
