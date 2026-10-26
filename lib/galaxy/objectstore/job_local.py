"""An object store that stages a job's outputs in the job's own directory.

A job's metadata step, run alongside the job with extended metadata, writes the
objects it creates through this store: each file is staged under ``files_dir``
and listed in ``manifest.jsonl`` there, one JSON record per line naming the
object (class, uuid, location within it) and the file by a path relative to
``files_dir``. Galaxy issues an upload target in its own object store for each
record, and Pulsar uploads the files after the job - the job never holds
storage credentials.
"""

import json
import os
import shutil
from typing import Any

from galaxy.exceptions import ConfigurationError
from galaxy.util import umask_fix_perms
from . import DiskObjectStore

MANIFEST_NAME = "manifest.jsonl"
# Path arguments that locate a file within an object, recorded so Galaxy can place it.
LOCATION_KEYS = ("extra_dir", "extra_dir_at_root", "alt_name")


class JobLocalObjectStore(DiskObjectStore):
    """Stage objects in a job directory and list them for upload.

    Objects are laid out by uuid, the identity Galaxy places them by; symlinks are
    staged as their contents.
    """

    store_type = "job_local"

    def __init__(self, config: Any, config_dict: dict[str, Any]) -> None:
        super().__init__(config, config_dict)
        if self.store_by != "uuid":
            raise ConfigurationError("A job_local object store must store objects by uuid.")
        self.manifest_path = os.path.join(self.file_path, MANIFEST_NAME)

    def _create(self, obj: Any, **kwargs: Any) -> "JobLocalObjectStore":
        new_file = not kwargs.get("dir_only", False) and not self._exists(obj, **kwargs)
        super()._create(obj, **kwargs)
        if new_file:
            self._list(obj, **kwargs)
        return self

    def _update_from_file(
        self,
        obj: Any,
        file_name: str | None = None,
        create: bool = False,
        preserve_symlinks: bool = False,
        **kwargs: Any,
    ) -> None:
        if create:
            self._create(obj, **kwargs)
        if file_name and self._exists(obj, **kwargs):
            self._stage(file_name, self._get_filename(obj, **kwargs))

    def _delete(self, obj: Any, entire_dir: bool = False, **kwargs: Any) -> bool:
        deleted = super()._delete(obj, entire_dir=entire_dir, **kwargs)
        self._unlist_missing()
        return deleted

    def _stage(self, file_name: str, path: str) -> None:
        source = os.path.realpath(file_name)
        if os.path.samefile(source, path):
            # Written in place, as datatypes write metadata files.
            return
        os.unlink(path)
        try:
            # Outputs can be large: share the job's file rather than copy it.
            os.link(source, path)
        except OSError:
            shutil.copyfile(source, path)
            umask_fix_perms(path, self.config.umask, 0o666)

    def _list(self, obj: Any, **kwargs: Any) -> None:
        record: dict[str, Any] = {
            "path": os.path.relpath(self._construct_path(obj, **kwargs), self.file_path),
            "class": type(obj).__name__,
            "uuid": str(obj.uuid),
            "object_store_id": obj.object_store_id,
        }
        record.update({key: kwargs[key] for key in LOCATION_KEYS if kwargs.get(key)})
        # A metadata file belongs to a dataset, whose store it is kept in.
        dataset = getattr(obj, "dataset", None)
        if dataset is not None:
            record["dataset_uuid"] = str(dataset.uuid)
        with open(self.manifest_path, "a") as manifest:
            manifest.write(json.dumps(record) + "\n")

    def _unlist_missing(self) -> None:
        if not os.path.exists(self.manifest_path):
            return
        with open(self.manifest_path) as manifest:
            records = [json.loads(line) for line in manifest]
        listed = [record for record in records if os.path.exists(os.path.join(self.file_path, record["path"]))]
        if len(listed) == len(records):
            return
        rewritten_path = f"{self.manifest_path}.tmp"
        with open(rewritten_path, "w") as rewritten:
            rewritten.writelines(json.dumps(record) + "\n" for record in listed)
        os.replace(rewritten_path, self.manifest_path)
