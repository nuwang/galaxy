from copy import deepcopy
from dataclasses import (
    dataclass,
    field,
    InitVar,
)
from typing import (
    Any,
    cast,
    TYPE_CHECKING,
    Union,
)

if TYPE_CHECKING:
    from galaxy.jobs import ResubmitConfigDict
    from galaxy.model import Job


JOB_DESTINATION_PARAMS_METADATA_KEY = "__galaxy_job_destination"


def job_destination_params_for_persistence(job_destination: "JobDestination") -> dict[str, Any]:
    params = deepcopy(job_destination.params)
    if job_destination.resubmit:
        metadata: dict[str, Any] = {"resubmit": deepcopy(job_destination.resubmit)}
        if job_destination.env:
            metadata["env"] = deepcopy(job_destination.env)
        if job_destination.tags:
            metadata["tags"] = deepcopy(job_destination.tags)
        if job_destination.shell:
            metadata["shell"] = job_destination.shell
        params[JOB_DESTINATION_PARAMS_METADATA_KEY] = metadata
    return params


@dataclass(kw_only=True, eq=False)
class JobDestination:
    """
    Provides details about where a job runs
    """

    id: Union[str, None] = None
    url: Union[str, None] = None
    tags: Union[list[str], None] = None
    runner: Union[str, None] = None
    legacy: bool = False
    converted: bool = False
    shell: Union[str, None] = None
    env: list[dict[str, Any]] = field(default_factory=list)
    resubmit: list["ResubmitConfigDict"] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    from_job: InitVar[Union["Job", None]] = None

    def __post_init__(self, from_job: Union["Job", None] = None) -> None:
        # Use the values persisted in an existing job
        if from_job is not None and from_job.destination_id is not None:
            self.id = from_job.destination_id
            self.params = deepcopy(from_job.destination_params or {})
            metadata = self.params.pop(JOB_DESTINATION_PARAMS_METADATA_KEY, None)
            if isinstance(metadata, dict):
                self.resubmit = cast(list["ResubmitConfigDict"], metadata.get("resubmit") or [])
                self.env = cast(list[dict[str, Any]], metadata.get("env") or [])
                self.tags = cast(Union[list[str], None], metadata.get("tags"))
                self.shell = cast(Union[str, None], metadata.get("shell"))
