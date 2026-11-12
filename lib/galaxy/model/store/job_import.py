"""Check the dataset uuids a job's extended-metadata import gives its outputs.

A job writes the model store its outputs are imported from. With datasets stored by uuid, a dataset's
uuid is where its file lives: an output keeps the uuid Galaxy gave it, and a new dataset must not take
another dataset's, or it would share that dataset's file.
"""

from typing import Any
from uuid import UUID

from sqlalchemy import select

from galaxy import model
from galaxy.exceptions import MalformedContents
from galaxy.model.store import ModelImportStore

OUTPUT_ASSOCIATIONS = ("output_datasets", "output_library_datasets")


def check_job_import_dataset_uuids(import_store: ModelImportStore, job: model.Job) -> None:
    """Raise MalformedContents if importing ``import_store`` would change an output's uuid or reuse a dataset's."""
    outputs: dict[tuple[str | None, str], model.Dataset | None] = {
        (type(association.dataset).__name__, str(association.dataset.id)): association.dataset.dataset
        for name in OUTPUT_ASSOCIATIONS
        for association in getattr(job, name)
        if association.dataset is not None
    }
    for dataset_attrs in import_store.datasets_properties():
        output = None
        if "id" in dataset_attrs:
            output = outputs.get((dataset_attrs.get("model_class"), str(dataset_attrs["id"])))
        # The importer takes a dataset's uuid from any of these.
        for uuid in (
            dataset_attrs.get("uuid"),
            dataset_attrs.get("dataset_uuid"),
            (dataset_attrs.get("dataset") or {}).get("uuid"),
        ):
            if uuid is not None:
                _check_uuid(import_store, _parse_uuid(uuid), output)


def _check_uuid(import_store: ModelImportStore, uuid: UUID, output: Any) -> None:
    if output is not None:
        if uuid != UUID(str(output.uuid)):
            raise MalformedContents("A job cannot change the uuid of its outputs.")
        return
    if import_store.sa_session.scalars(select(model.Dataset.id).filter_by(uuid=uuid).limit(1)).first() is not None:
        raise MalformedContents("A job cannot give a new dataset the uuid of another dataset.")


def _parse_uuid(uuid: Any) -> UUID:
    try:
        return UUID(str(uuid))
    except ValueError:
        raise MalformedContents(f"Invalid uuid [{uuid}].")
