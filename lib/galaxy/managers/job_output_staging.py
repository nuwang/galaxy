"""Issue a running job's remote runner upload targets for the outputs it staged.

With ``object_store_staging``, a job's metadata step stages its outputs in the
job's own directory (``job_local`` object store) and lists them; after the job,
Pulsar sends Galaxy that list (``output_targets_url``) and uploads each file to
the target Galaxy answers with: a presigned object store URL where the store
issues one, else a signed Galaxy URL that writes the upload to the store. The
job never holds storage credentials, and Galaxy decides where each file goes:
only into this job's outputs, or into new datasets under unused uuids in a
store the job was given.
"""

import base64
import json
import logging
import os
import posixpath
import tempfile
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import (
    Any,
    Literal,
)
from urllib.parse import quote
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)
from sqlalchemy import select

from galaxy import exceptions
from galaxy.config import GalaxyAppConfiguration
from galaxy.model import (
    Dataset,
    Job,
    MetadataFile,
)
from galaxy.model.scoped_session import galaxy_scoped_session
from galaxy.objectstore import (
    BaseObjectStore,
    CompletedPart,
    MultipartUploadTarget,
    PutUploadTarget,
)
from galaxy.security.idencoding import IdEncodingHelper
from galaxy.security.staging_signer import StagingUrlSigner

log = logging.getLogger(__name__)

# The targets URL is used when the job ends, which may be weeks after it was queued;
# the job leaving Job.non_ready_states ends it sooner.
OUTPUT_TARGETS_URL_LIFETIME_SECONDS = 30 * 24 * 60 * 60
# Galaxy's own upload URLs are used right after they are issued, in turn for a large file's parts.
UPLOAD_URL_LIFETIME_SECONDS = 24 * 60 * 60
METADATA_FILES_DIR = "_metadata_files"


class StagedOutputFile(BaseModel):
    """A file a job staged for upload, as the ``job_local`` object store listed it, with its size."""

    model_config = ConfigDict(populate_by_name=True)

    class_: Literal["Dataset", "MetadataFile"] = Field(alias="class")
    uuid: UUID
    object_store_id: str | None = None
    extra_dir: str | None = None
    extra_dir_at_root: bool = False
    alt_name: str | None = None
    # For a metadata file: the dataset it belongs to.
    dataset_uuid: UUID | None = None
    size: int = Field(ge=0)


class OutputTargetsRequest(BaseModel):
    files: list[StagedOutputFile]


class OutputTargetsResponse(BaseModel):
    targets: list[dict[str, Any]]


class UploadedPart(BaseModel):
    part_number: int = Field(ge=1)
    etag: str


class CompleteUploadRequest(BaseModel):
    parts: list[UploadedPart]


@dataclass(frozen=True)
class StagedObject:
    """An object Galaxy accepted an upload for and where it goes: what one of Galaxy's upload URLs binds."""

    class_: str
    uuid: str
    object_store_id: str | None
    location: dict[str, Any]

    def model_object(self) -> Dataset | MetadataFile:
        obj = Dataset(uuid=self.uuid) if self.class_ == "Dataset" else MetadataFile(uuid=self.uuid)
        obj.object_store_id = self.object_store_id
        return obj

    def to_dict(self) -> dict[str, Any]:
        return {
            "class": self.class_,
            "uuid": self.uuid,
            "object_store_id": self.object_store_id,
            "location": self.location,
        }

    @staticmethod
    def from_dict(as_dict: dict[str, Any]) -> "StagedObject":
        return StagedObject(as_dict["class"], as_dict["uuid"], as_dict["object_store_id"], as_dict["location"])


@dataclass
class ObjectUpload:
    """An upload through Galaxy in progress: the request body goes to ``path``, and must be ``size`` bytes."""

    path: str
    size: int
    staged: StagedObject

    def discard(self) -> None:
        with suppress(FileNotFoundError):
            os.unlink(self.path)


def output_targets_url(security: IdEncodingHelper, galaxy_url: str, job_id: int, expires: int) -> str:
    """A signed URL at which a job's remote runner asks for upload targets, answered by ``issue_targets``."""
    signature = StagingUrlSigner(security.id_secret).sign(job_id, "output_targets", job_id, "", expires)
    encoded_job_id = security.encode_id(job_id)
    return f"{galaxy_url}/api/jobs/{encoded_job_id}/staging/outputs/targets?exp={expires}&sig={signature}"


class JobOutputStagingManager:
    def __init__(
        self,
        sa_session: galaxy_scoped_session,
        object_store: BaseObjectStore,
        security: IdEncodingHelper,
        config: GalaxyAppConfiguration,
    ) -> None:
        self._sa_session = sa_session
        self._object_store = object_store
        self._signer = StagingUrlSigner(security.id_secret)
        self._config = config

    def issue_targets(
        self, job_id: int, expires: int, signature: str, files: list[StagedOutputFile]
    ) -> list[dict[str, Any]]:
        """One upload target per staged file, in order; refuses the request if any file may not be uploaded."""
        job = self._active_job(job_id, "output_targets", "", expires, signature)
        staged_objects = self._accepted_objects(job, files)
        upload_expires = int(time.time()) + UPLOAD_URL_LIFETIME_SECONDS
        return [self._target(job_id, staged, file.size, upload_expires) for staged, file in zip(staged_objects, files)]

    def complete_upload(
        self, job_id: int, target: str, expires: int, signature: str, parts: list[CompletedPart]
    ) -> None:
        payload = self._verified_upload(job_id, "complete", target, expires, signature)
        staged = StagedObject.from_dict(payload)
        self._object_store.complete_upload(staged.model_object(), payload["upload_id"], parts, **staged.location)

    def start_object_upload(self, job_id: int, target: str, expires: int, signature: str) -> ObjectUpload:
        payload = self._verified_upload(job_id, "object", target, expires, signature)
        os.makedirs(self._config.new_file_path, exist_ok=True)
        fd, path = tempfile.mkstemp(prefix="job_output_upload_", dir=self._config.new_file_path)
        os.close(fd)
        return ObjectUpload(path=path, size=payload["size"], staged=StagedObject.from_dict(payload))

    def finish_object_upload(self, upload: ObjectUpload) -> None:
        received = os.path.getsize(upload.path)
        if received != upload.size:
            raise exceptions.RequestParameterInvalidException(
                f"Upload of {received} bytes refused: Galaxy accepted {upload.size}."
            )
        staged = upload.staged
        self._object_store.update_from_file(
            staged.model_object(), file_name=upload.path, create=True, **staged.location
        )

    def _active_job(self, job_id: int, kind: str, path: str, expires: int, signature: str) -> Job:
        if not self._signer.verify(job_id, kind, job_id, path, expires, signature):
            raise exceptions.ItemAccessibilityException("Invalid or expired staging URL.")
        job = self._sa_session.get(Job, job_id)
        if job is None or job.state not in Job.non_ready_states:
            raise exceptions.ItemAccessibilityException("Attempting to stage an output of a job that is not active.")
        return job

    def _verified_upload(self, job_id: int, action: str, target: str, expires: int, signature: str) -> dict[str, Any]:
        self._active_job(job_id, f"output_{action}", target, expires, signature)
        payload: dict[str, Any] = json.loads(base64.urlsafe_b64decode(target))
        return payload

    def _accepted_objects(self, job: Job, files: list[StagedOutputFile]) -> list[StagedObject]:
        outputs = {dataset.uuid: dataset for dataset in _output_datasets(job)}
        dataset_uuids: set[UUID | None] = {file.uuid for file in files if file.class_ == "Dataset"}
        dataset_uuids |= {file.dataset_uuid for file in files if file.class_ == "MetadataFile"}
        new_dataset_uuids = dataset_uuids - outputs.keys()
        max_discovered_files = self._config.max_discovered_files
        if max_discovered_files is not None and len(new_dataset_uuids) > max_discovered_files:
            raise exceptions.RequestParameterInvalidException(
                f"Job {job.id} staged {len(new_dataset_uuids)} new datasets, more than {max_discovered_files}."
            )
        if self._existing_dataset_uuids(new_dataset_uuids):
            raise exceptions.ItemAccessibilityException(f"Job {job.id} cannot upload to datasets that are not its own.")
        metadata_file_owners = self._metadata_file_owners(
            {file.uuid for file in files if file.class_ == "MetadataFile"}
        )
        new_object_store_ids = {
            job.object_store_id,
            *(job.object_store_id_overrides or {}).values(),
            *(dataset.object_store_id for dataset in outputs.values()),
        }

        def object_store_id(dataset_uuid: UUID | None, requested: str | None) -> str | None:
            """Outputs stay where Galaxy put them; new datasets go to a store the job was given."""
            if dataset_uuid in outputs:
                return outputs[dataset_uuid].object_store_id
            if requested not in new_object_store_ids:
                raise exceptions.ItemAccessibilityException(
                    f"Job {job.id} cannot store new datasets in object store [{requested}]."
                )
            return requested

        accepted = []
        for file in files:
            location = _location(file)
            if file.class_ == "Dataset":
                store_id = object_store_id(file.uuid, file.object_store_id)
            else:
                owners = metadata_file_owners.get(file.uuid)
                if owners is not None and owners != {file.dataset_uuid}:
                    raise exceptions.ItemAccessibilityException(
                        f"Job {job.id} cannot upload the metadata files of another dataset."
                    )
                store_id = object_store_id(file.dataset_uuid, file.object_store_id)
            accepted.append(StagedObject(file.class_, str(file.uuid), store_id, location))
        return accepted

    def _existing_dataset_uuids(self, uuids: set[Any]) -> set[UUID]:
        if not uuids:
            return set()
        return set(self._sa_session.scalars(select(Dataset.uuid).where(Dataset.uuid.in_(uuids))))

    def _metadata_file_owners(self, uuids: set[UUID]) -> dict[UUID, set[UUID | None]]:
        """The uuids of the datasets existing metadata files with these uuids belong to."""
        owners: dict[UUID, set[UUID | None]] = {}
        if not uuids:
            return owners
        for metadata_file in self._sa_session.scalars(select(MetadataFile).where(MetadataFile.uuid.in_(uuids))):
            dataset = metadata_file.dataset
            owner = UUID(str(dataset.uuid)) if dataset is not None and dataset.uuid is not None else None
            owners.setdefault(UUID(str(metadata_file.uuid)), set()).add(owner)
        return owners

    def _target(self, job_id: int, staged: StagedObject, size: int, upload_expires: int) -> dict[str, Any]:
        upload_target = self._object_store.get_upload_target(staged.model_object(), size, **staged.location)
        if isinstance(upload_target, PutUploadTarget):
            return {"type": "put", "url": upload_target.url}
        if isinstance(upload_target, MultipartUploadTarget):
            payload = {**staged.to_dict(), "upload_id": upload_target.upload_id}
            return {
                "type": "multipart",
                "parts": [dict(part) for part in upload_target.parts],
                "complete_url": self._upload_url(job_id, "complete", payload, upload_expires),
            }
        # The store takes no direct uploads: the runner uploads to Galaxy, which writes to the store.
        return {
            "type": "put",
            "url": self._upload_url(job_id, "object", {**staged.to_dict(), "size": size}, upload_expires),
        }

    def _upload_url(self, job_id: int, action: str, payload: dict[str, Any], expires: int) -> str:
        """A signed URL for an upload Galaxy handles, relative to the targets URL the runner resolves it against."""
        target = base64.urlsafe_b64encode(json.dumps(payload, sort_keys=True).encode()).decode()
        signature = self._signer.sign(job_id, f"output_{action}", job_id, target, expires)
        return f"{action}?target={quote(target)}&exp={expires}&sig={signature}"


def _output_datasets(job: Job) -> list[Dataset]:
    instances = [
        *(association.dataset for association in job.output_datasets),
        *(association.dataset for association in job.output_library_datasets),
    ]
    return [instance.dataset for instance in instances if instance is not None and instance.dataset is not None]


def _location(file: StagedOutputFile) -> dict[str, Any]:
    """Where in its object a file goes, if it is somewhere the job may write: refuses anywhere else.

    Objects are stored by uuid, so the location must stay within the file's own object: a dataset's
    primary file or its extra files directory, or a metadata file's own name.
    """
    if file.class_ == "Dataset":
        if file.extra_dir is None and file.alt_name is None and not file.extra_dir_at_root:
            return {}
        if (
            not file.extra_dir_at_root
            and file.extra_dir is not None
            and _within(file.extra_dir, f"dataset_{file.uuid}_files")
            and file.alt_name is not None
            and _is_file_name(file.alt_name)
        ):
            return {"extra_dir": file.extra_dir, "alt_name": file.alt_name}
    elif (
        file.dataset_uuid is not None
        and file.extra_dir == METADATA_FILES_DIR
        and file.extra_dir_at_root
        and file.alt_name == f"metadata_{file.uuid}.dat"
    ):
        return {"extra_dir": METADATA_FILES_DIR, "extra_dir_at_root": True, "alt_name": file.alt_name}
    raise exceptions.RequestParameterInvalidException(f"A job cannot upload {file.class_} {file.uuid} there.")


def _within(path: str, directory: str) -> bool:
    return path == posixpath.normpath(path) and (path == directory or path.startswith(f"{directory}/"))


def _is_file_name(name: str) -> bool:
    return name not in ("", ".", "..") and "/" not in name and "\0" not in name
