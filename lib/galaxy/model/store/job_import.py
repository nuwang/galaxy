"""Check a job's extended-metadata import before it is applied.

With extended metadata, a job exports its outputs as a model store
(``metadata/outputs_populated``) that Galaxy imports with editing allowed.
The store is written from the job's own working directory, so it is only as
trustworthy as the job: it may edit this job and its outputs, create new
datasets and collections for it, and add to the library folders it was asked to
upload into, and nothing else.
"""

import json
from collections.abc import Iterator
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm.scoping import scoped_session

from galaxy import model
from galaxy.exceptions import MalformedContents
from galaxy.model.store import (
    ModelImportStore,
    SessionlessContext,
)
from galaxy.objectstore import is_user_object_store


def validate_job_import(import_store: ModelImportStore, job: model.Job, allow_external_filename: bool = False) -> None:
    """Raise MalformedContents if importing ``import_store`` would reach beyond ``job`` and its outputs.

    ``allow_external_filename`` lets an output keep the path of a file on Galaxy's disk, which only
    an upload linking a server file (link_data_only) legitimately does.
    """
    sa_session = import_store.sa_session
    assert not isinstance(sa_session, SessionlessContext)
    if import_store.invocations_properties():
        raise MalformedContents("A job's outputs cannot include workflow invocations.")
    upload_folder_ids = {str(folder_id) for folder_id in _upload_folder_ids(job)}
    for library_attrs in import_store.library_properties():
        if library_attrs.get("model_class") != "LibraryFolder" or _id(library_attrs.get("id")) not in upload_folder_ids:
            raise MalformedContents(f"Job {job.id} can only add to the library folders it was asked to upload into.")

    output_instances: list[model.DatasetInstance] = [
        *(association.dataset for association in job.output_datasets if association.dataset is not None),
        *(association.dataset for association in job.output_library_datasets if association.dataset is not None),
    ]
    outputs: dict[tuple[Any, str | None], model.DatasetInstance] = {
        (type(instance).__name__, _id(instance.id)): instance for instance in output_instances
    }
    output_object_store_ids = {instance.dataset.object_store_id for instance in outputs.values() if instance.dataset}
    for dataset_attrs in import_store.datasets_properties():
        instance = None
        if "id" in dataset_attrs:
            instance = outputs.get((dataset_attrs.get("model_class"), _id(dataset_attrs["id"])))
            if instance is None:
                raise MalformedContents(f"Job {job.id} cannot edit dataset {dataset_attrs['id']}: not its output.")
        elif "dataset" not in dataset_attrs:
            raise MalformedContents("A job's new outputs must be stored in the object store, not read from files.")
        existing = instance.dataset if instance is not None else None
        if "dataset" in dataset_attrs:
            _validate_dataset_fields(
                sa_session, dataset_attrs["dataset"], existing, output_object_store_ids, allow_external_filename
            )
        for uuid in (dataset_attrs.get("uuid"), dataset_attrs.get("dataset_uuid")):
            _validate_dataset_uuid(sa_session, uuid, existing)
        for metadata_file_uuid in _metadata_file_uuids(dataset_attrs.get("metadata")):
            _validate_metadata_file_uuid(sa_session, metadata_file_uuid, instance)

    _validate_collections(import_store.collections_properties(), job)
    for job_attrs in import_store.jobs_properties():
        # The importer looks jobs up by id, so the same job may be exported with an int or a str id.
        if _id(job_attrs.get("id")) != str(job.id):
            raise MalformedContents(f"Job {job.id} cannot edit or create other jobs.")


def _id(value: Any) -> str | None:
    """An exported database id in the form the importer's lookups treat alike."""
    return None if value is None else str(value)


def _upload_folder_ids(job: model.Job) -> set[int]:
    """Library folders the job may add datasets to: its own, or those a data fetch request targets.

    Galaxy checked the user's access to these folders when it created the job. Only the data fetch tool's
    request is read: any other tool could name a folder through a parameter of the same name.
    """
    folder_ids = {job.library_folder_id} if job.library_folder_id is not None else set()
    if job.tool_id == "__DATA_FETCH__":
        request_json = next((p.value for p in job.parameters if p.name == "request_json"), None)
        if request_json:
            for target in json.loads(json.loads(request_json)).get("targets", []):
                destination = target.get("destination") or {}
                if destination.get("type") == "library_folder":
                    folder_ids.add(destination["library_folder_id"])
    return folder_ids


def _validate_dataset_fields(
    sa_session: scoped_session[Any],
    fields: dict[str, Any],
    existing: model.Dataset | None,
    output_object_store_ids: set[str | None],
    allow_external_filename: bool,
) -> None:
    if fields.get("id") is not None and (existing is None or _id(fields["id"]) != str(existing.id)):
        raise MalformedContents("A job cannot give its outputs the database id of another dataset.")
    external_filename = fields.get("external_filename")
    if external_filename and not allow_external_filename:
        if existing is None or external_filename != existing.external_filename:
            raise MalformedContents("A job cannot point its outputs at files on Galaxy's disk.")
    if "object_store_id" in fields:
        object_store_id = fields["object_store_id"]
        if existing is not None and object_store_id != existing.object_store_id:
            raise MalformedContents("A job cannot move its outputs to another object store.")
        if is_user_object_store(object_store_id) and object_store_id not in output_object_store_ids:
            raise MalformedContents("A job can only store new outputs in the user object stores it was given.")
    _validate_dataset_uuid(sa_session, fields.get("uuid"), existing)


def _validate_dataset_uuid(sa_session: scoped_session[Any], uuid: str | None, existing: model.Dataset | None) -> None:
    """An output keeps the uuid Galaxy gave it; a new dataset's uuid must not be another dataset's.

    With datasets stored by uuid, the uuid is where a dataset's file lives.
    """
    if uuid is None:
        return
    if existing is not None:
        if _parse_uuid(uuid) != _parse_uuid(str(existing.uuid)):
            raise MalformedContents("A job cannot change the uuid of its outputs.")
        return
    if sa_session.scalars(select(model.Dataset.id).filter_by(uuid=_parse_uuid(uuid)).limit(1)).first() is not None:
        raise MalformedContents("A job cannot give its outputs the uuid of another dataset.")


def _validate_metadata_file_uuid(
    sa_session: scoped_session[Any], uuid: str, instance: model.DatasetInstance | None
) -> None:
    """A metadata file is new, or already belongs to the output it is exported with."""
    for metadata_file in sa_session.scalars(select(model.MetadataFile).filter_by(uuid=_parse_uuid(uuid))):
        if instance is None or instance not in (metadata_file.history_dataset, metadata_file.library_dataset):
            raise MalformedContents("A job cannot give its outputs the metadata files of another dataset.")


def _metadata_file_uuids(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        if value.get("model_class") == "MetadataFile":
            yield value["uuid"]
        else:
            for item in value.values():
                yield from _metadata_file_uuids(item)
    elif isinstance(value, list):
        for item in value:
            yield from _metadata_file_uuids(item)


def _parse_uuid(uuid: str) -> UUID:
    try:
        return UUID(str(uuid))
    except ValueError:
        raise MalformedContents(f"Invalid uuid [{uuid}].")


def _validate_collections(collections_attrs: list[dict[str, Any]], job: model.Job) -> None:
    output_hdcas = [assoc.dataset_collection_instance for assoc in job.output_dataset_collection_instances]
    output_collections = [
        *(hdca.collection for hdca in output_hdcas),
        *(assoc.dataset_collection for assoc in job.output_dataset_collections),
    ]
    output_hdca_ids = {str(hdca.id) for hdca in output_hdcas}
    output_collection_ids = {str(collection.id) for collection in _with_child_collections(output_collections)}

    def validate_collection(collection_attrs: dict[str, Any]) -> None:
        if collection_attrs.get("id") is not None and _id(collection_attrs["id"]) not in output_collection_ids:
            raise MalformedContents(f"Job {job.id} cannot edit collection {collection_attrs['id']}: not its output.")
        for element_attrs in collection_attrs.get("elements") or []:
            if "child_collection" in element_attrs:
                validate_collection(element_attrs["child_collection"])

    for collection_attrs in collections_attrs:
        if "collection" in collection_attrs:
            if collection_attrs.get("id") is not None and _id(collection_attrs["id"]) not in output_hdca_ids:
                raise MalformedContents(
                    f"Job {job.id} cannot edit collection {collection_attrs['id']}: not its output."
                )
            validate_collection(collection_attrs["collection"])
        else:
            validate_collection(collection_attrs)


def _with_child_collections(collections: list[model.DatasetCollection]) -> Iterator[model.DatasetCollection]:
    for collection in collections:
        yield collection
        yield from _with_child_collections(
            [element.child_collection for element in collection.elements if element.child_collection is not None]
        )
