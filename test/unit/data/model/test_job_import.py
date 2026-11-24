"""A job's extended-metadata import keeps its outputs' uuids, and gives new datasets unused ones.

With datasets stored by uuid, a dataset's uuid is where its file lives.
"""

import json
import os
from tempfile import NamedTemporaryFile

import pytest

from galaxy import model
from galaxy.exceptions import MalformedContents
from galaxy.model import store
from galaxy.model.store.job_import import check_job_import_dataset_uuids
from galaxy.model.unittest_utils import GalaxyDataTestApp
from galaxy.objectstore.unittest_utils import Config as TestConfig

UNUSED_UUID = "0b8d2f1e-0f3e-4a8e-9a55-3c1d7e0f6a21"


@pytest.fixture
def app():
    app = GalaxyDataTestApp()
    app.object_store = TestConfig(store_by="uuid").object_store
    model.Dataset.object_store = app.object_store
    return app


def _hda(app, history):
    hda = model.HistoryDatasetAssociation(
        extension="txt", history=history, create_dataset=True, sa_session=app.model.session
    )
    hda.state = hda.states.OK
    app.model.session.add(hda)
    app.model.session.commit()
    with NamedTemporaryFile("w") as f:
        f.write("content\n")
        f.flush()
        app.object_store.update_from_file(hda.dataset, file_name=f.name, create=True)
    return hda


@pytest.fixture
def job(app):
    """A cat1 job with one input and one output, as extended metadata sees it when the job finishes."""
    user = model.User(email="owner@example.com", password="password")
    history = model.History(name="history", user=user)
    job = model.Job()
    job.user = user
    job.history = history
    job.tool_id = "cat1"
    job.state = job.states.RUNNING
    job.add_input_dataset("input1", _hda(app, history))
    job.add_output_dataset("out_file1", _hda(app, history))
    app.model.session.add(job)
    app.model.session.commit()
    return job


def _other_uuid(job) -> str:
    return str(job.input_datasets[0].dataset.dataset.uuid)


def _export_outputs(directory, app, job, edit=None):
    """Export the job's outputs as the metadata script writes metadata/outputs_populated, edited by the job."""
    with store.DirectoryModelExportStore(
        directory,
        app=app,
        for_edit=True,
        serialize_dataset_objects=True,
        strip_metadata_files=False,
        serialize_jobs=True,
    ) as export_store:
        for association in job.output_datasets:
            export_store.add_dataset(association.dataset)
        export_store.export_job(job, include_job_data=False)
    if edit:
        path = os.path.join(directory, store.ATTRS_FILENAME_DATASETS)
        with open(path) as f:
            datasets_attrs = json.load(f)
        edit(datasets_attrs)
        with open(path, "w") as f:
            json.dump(datasets_attrs, f)
    return store.get_import_model_store_for_directory(
        directory,
        app=app,
        user=job.user,
        import_options=store.ImportOptions(allow_dataset_object_edit=True, allow_edit=True),
    )


def _new_dataset_like_output(datasets_attrs, field="dataset.uuid", uuid=UNUSED_UUID):
    """A discovered output: a new dataset with the job's output as a template, under ``uuid``."""
    new = json.loads(json.dumps(datasets_attrs[0]))
    del new["id"]
    new.pop("dataset_uuid", None)
    new.pop("uuid", None)
    new["encoded_id"] = "discovered"
    new["dataset"] = {**new["dataset"], "id": None, "uuid": UNUSED_UUID}
    _set_uuid(new, field, uuid)
    datasets_attrs.append(new)


def _set_uuid(dataset_attrs, field, uuid):
    if field == "dataset.uuid":
        dataset_attrs["dataset"]["uuid"] = uuid
    else:
        dataset_attrs[field] = uuid


UUID_FIELDS = ["dataset.uuid", "uuid", "dataset_uuid"]


def test_export_of_the_jobs_own_outputs_is_accepted(tmp_path, app, job):
    check_job_import_dataset_uuids(_export_outputs(tmp_path, app, job), job)


def test_new_dataset_under_an_unused_uuid_is_accepted(tmp_path, app, job):
    check_job_import_dataset_uuids(_export_outputs(tmp_path, app, job, _new_dataset_like_output), job)


@pytest.mark.parametrize("field", UUID_FIELDS)
def test_changing_an_outputs_uuid_is_refused(tmp_path, app, job, field):
    # Galaxy chose the uuid when it created the output.
    import_store = _export_outputs(tmp_path, app, job, lambda attrs: _set_uuid(attrs[0], field, UNUSED_UUID))
    with pytest.raises(MalformedContents):
        check_job_import_dataset_uuids(import_store, job)


@pytest.mark.parametrize("field", UUID_FIELDS)
def test_new_dataset_taking_another_datasets_uuid_is_refused(tmp_path, app, job, field):
    # It would share the other dataset's file: the job could read it, and purging either deletes it.
    import_store = _export_outputs(
        tmp_path, app, job, lambda attrs: _new_dataset_like_output(attrs, field, _other_uuid(job))
    )
    with pytest.raises(MalformedContents):
        check_job_import_dataset_uuids(import_store, job)


def test_invalid_uuid_is_refused(tmp_path, app, job):
    import_store = _export_outputs(tmp_path, app, job, lambda attrs: _new_dataset_like_output(attrs, uuid="not-a-uuid"))
    with pytest.raises(MalformedContents):
        check_job_import_dataset_uuids(import_store, job)
