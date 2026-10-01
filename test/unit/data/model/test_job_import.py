"""A job's extended-metadata import may only edit that job and its outputs."""

import json
import os
from tempfile import NamedTemporaryFile

import pytest

from galaxy import model
from galaxy.exceptions import MalformedContents
from galaxy.model import store
from galaxy.model.store.job_import import validate_job_import
from galaxy.model.unittest_utils import GalaxyDataTestApp
from galaxy.objectstore.unittest_utils import Config as TestConfig


@pytest.fixture
def app():
    app = GalaxyDataTestApp()
    app.object_store = TestConfig(store_by="uuid").object_store
    model.Dataset.object_store = app.object_store
    return app


def _hda(app, history, content="content\n"):
    hda = model.HistoryDatasetAssociation(
        extension="txt", history=history, create_dataset=True, sa_session=app.model.session
    )
    hda.state = hda.states.OK
    app.model.session.add(hda)
    app.model.session.commit()
    with NamedTemporaryFile("w") as f:
        f.write(content)
        f.flush()
        app.object_store.update_from_file(hda.dataset, file_name=f.name, create=True)
    return hda


def _running_job(app, user, history):
    """A cat1 job with one input and one output, as extended metadata sees it when the job finishes."""
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


@pytest.fixture
def user(app):
    user = model.User(email="owner@example.com", password="password")
    app.model.session.add(user)
    app.model.session.commit()
    return user


@pytest.fixture
def history(app, user):
    history = model.History(name="history", user=user)
    app.model.session.add(history)
    app.model.session.commit()
    return history


@pytest.fixture
def job(app, user, history):
    return _running_job(app, user, history)


def _export_outputs(directory, app, job):
    """Export the job's outputs the way the metadata script writes metadata/outputs_populated."""
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
    return directory


def _import_store(directory, app, job):
    return store.get_import_model_store_for_directory(
        directory,
        app=app,
        user=job.user,
        import_options=store.ImportOptions(allow_dataset_object_edit=True, allow_edit=True),
    )


def _rewrite(directory, attrs_filename, edit):
    path = os.path.join(directory, attrs_filename)
    with open(path) as f:
        attrs = json.load(f)
    edit(attrs)
    with open(path, "w") as f:
        json.dump(attrs, f)


def test_export_of_the_jobs_own_outputs_is_accepted(tmp_path, app, job):
    directory = _export_outputs(tmp_path, app, job)
    validate_job_import(_import_store(directory, app, job), job)


def test_editing_a_dataset_that_is_not_an_output_of_the_job_is_refused(tmp_path, app, job, history):
    other = _hda(app, history)
    directory = _export_outputs(tmp_path, app, job)

    def point_at_other(datasets_attrs):
        datasets_attrs[0]["id"] = other.id

    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, point_at_other)
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def test_editing_another_job_is_refused(tmp_path, app, job, user, history):
    other_job = _running_job(app, user, history)
    directory = _export_outputs(tmp_path, app, job)

    def point_at_other_job(jobs_attrs):
        jobs_attrs[0]["id"] = other_job.id

    _rewrite(directory, store.ATTRS_FILENAME_JOBS, point_at_other_job)
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def _hdca(app, history, name="collection"):
    hdca = model.HistoryDatasetCollectionAssociation(
        collection=model.DatasetCollection(collection_type="list", populated=False),
        history=history,
        name=name,
    )
    app.model.session.add(hdca)
    app.model.session.commit()
    return hdca


def _export_outputs_with_collection(directory, app, job, history):
    hdca = _hdca(app, history, name="output collection")
    job.add_output_dataset_collection("output", hdca)
    app.model.session.commit()
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
        export_store.export_collection(hdca)
        export_store.export_job(job, include_job_data=False)
    return directory


def test_export_of_the_jobs_own_output_collection_is_accepted(tmp_path, app, job, history):
    directory = _export_outputs_with_collection(tmp_path, app, job, history)
    validate_job_import(_import_store(directory, app, job), job)


@pytest.mark.parametrize("edited", ["history_dataset_collection", "dataset_collection"])
def test_editing_a_collection_that_is_not_an_output_of_the_job_is_refused(tmp_path, app, job, history, edited):
    other = _hdca(app, history, name="someone else's")
    directory = _export_outputs_with_collection(tmp_path, app, job, history)

    def point_at_other(collections_attrs):
        if edited == "history_dataset_collection":
            collections_attrs[0]["id"] = other.id
        else:
            collections_attrs[0]["collection"]["id"] = other.collection.id

    _rewrite(directory, store.ATTRS_FILENAME_COLLECTIONS, point_at_other)
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


@pytest.mark.parametrize("attrs_filename", [store.ATTRS_FILENAME_LIBRARIES, store.ATTRS_FILENAME_INVOCATIONS])
def test_new_libraries_and_invocations_are_refused(tmp_path, app, job, attrs_filename):
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, attrs_filename, lambda attrs: attrs.append({"name": "not from this job"}))
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def _new_dataset_like_output(datasets_attrs, **dataset_fields):
    """A discovered output: a new dataset with the job's output as a template."""
    new = json.loads(json.dumps(datasets_attrs[0]))
    del new["id"]
    new.pop("dataset_uuid", None)
    new["encoded_id"] = "discovered"
    new["dataset"] = {**new["dataset"], "id": None, "uuid": "5f4f7b3e-5f1c-4b36-9d3b-0d8e8f5b2c11"}
    new["dataset"].update(dataset_fields)
    datasets_attrs.append(new)


def test_new_dataset_is_accepted(tmp_path, app, job):
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, _new_dataset_like_output)
    validate_job_import(_import_store(directory, app, job), job)


def test_new_dataset_read_from_a_file_rather_than_the_object_store_is_refused(tmp_path, app, job):
    directory = _export_outputs(tmp_path, app, job)

    def file_based(datasets_attrs):
        _new_dataset_like_output(datasets_attrs)
        del datasets_attrs[-1]["dataset"]
        datasets_attrs[-1]["file_name"] = "datasets/discovered.txt"

    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, file_based)
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def _set_output_dataset_field(field, value):
    def edit(datasets_attrs):
        datasets_attrs[0]["dataset"][field] = value

    return edit


def test_pointing_an_output_at_a_file_on_galaxys_disk_is_refused(tmp_path, app, job):
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, _set_output_dataset_field("external_filename", "/some/path"))
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def test_linked_output_file_is_accepted_when_allowed(tmp_path, app, job):
    # A local upload with link_data_only keeps the path of the linked server file.
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, _set_output_dataset_field("external_filename", "/linked/file"))
    validate_job_import(_import_store(directory, app, job), job, allow_external_filename=True)


def test_moving_an_output_to_another_object_store_is_refused(tmp_path, app, job):
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, _set_output_dataset_field("object_store_id", "elsewhere"))
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def test_new_dataset_in_a_user_object_store_the_job_was_not_given_is_refused(tmp_path, app, job):
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(
        directory,
        store.ATTRS_FILENAME_DATASETS,
        lambda attrs: _new_dataset_like_output(attrs, object_store_id="user_objects://someone-else"),
    )
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


UNUSED_UUID = "0b8d2f1e-0f3e-4a8e-9a55-3c1d7e0f6a21"


def _set_output_uuid(field):
    def edit(datasets_attrs):
        if field == "dataset.uuid":
            datasets_attrs[0]["dataset"]["uuid"] = UNUSED_UUID
        else:
            datasets_attrs[0][field] = UNUSED_UUID

    return edit


@pytest.mark.parametrize("field", ["dataset.uuid", "uuid", "dataset_uuid"])
def test_changing_an_outputs_uuid_is_refused(tmp_path, app, job, field):
    # Galaxy chose the uuid when it created the output: it is where the output's file lives.
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, _set_output_uuid(field))
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def test_output_taking_another_datasets_uuid_is_refused(tmp_path, app, job):
    other_uuid = str(job.input_datasets[0].dataset.dataset.uuid)
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, _set_output_dataset_field("uuid", other_uuid))
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def test_new_dataset_taking_another_datasets_uuid_is_refused(tmp_path, app, job):
    other_uuid = str(job.input_datasets[0].dataset.dataset.uuid)
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, lambda attrs: _new_dataset_like_output(attrs, uuid=other_uuid))
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def test_output_taking_another_datasets_database_id_is_refused(tmp_path, app, job):
    other_id = job.input_datasets[0].dataset.dataset.id
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, _set_output_dataset_field("id", other_id))
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def _metadata_file(uuid):
    return {"model_class": "MetadataFile", "uuid": uuid}


def test_output_with_a_new_metadata_file_is_accepted(tmp_path, app, job):
    directory = _export_outputs(tmp_path, app, job)

    def add_index(datasets_attrs):
        datasets_attrs[0]["metadata"]["bam_index"] = _metadata_file("7c9e6679-7425-40de-944b-e07fc1f90ae7")

    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, add_index)
    validate_job_import(_import_store(directory, app, job), job)


def test_output_taking_another_datasets_metadata_file_is_refused(tmp_path, app, job):
    other = job.input_datasets[0].dataset
    other_index = model.MetadataFile(dataset=other, name="bam_index")
    app.model.session.add(other_index)
    app.model.session.commit()
    directory = _export_outputs(tmp_path, app, job)

    def take_index(datasets_attrs):
        datasets_attrs[0]["metadata"]["bam_index"] = _metadata_file(str(other_index.uuid))

    _rewrite(directory, store.ATTRS_FILENAME_DATASETS, take_index)
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def _library_folder(app):
    folder = model.LibraryFolder(name="uploads")
    app.model.session.add(folder)
    app.model.session.commit()
    return folder


def _fetch_into(job, folder, tool_id="__DATA_FETCH__"):
    """Make ``job`` a data fetch job asked to upload into ``folder``, as the fetch API records it."""
    job.tool_id = tool_id
    request = {"targets": [{"destination": {"type": "library_folder", "library_folder_id": folder.id}}]}
    job.parameters = [model.JobParameter(name="request_json", value=json.dumps(json.dumps(request)))]


def _add_to_folder(folder_id):
    def edit(libraries_attrs):
        libraries_attrs.append(
            {"model_class": "LibraryFolder", "id": folder_id, "name": "uploads", "datasets": [], "folders": []}
        )

    return edit


def test_data_fetch_adding_to_the_folder_it_was_asked_to_upload_into_is_accepted(tmp_path, app, job):
    folder = _library_folder(app)
    _fetch_into(job, folder)
    app.model.session.commit()
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_LIBRARIES, _add_to_folder(folder.id))
    validate_job_import(_import_store(directory, app, job), job)


def test_data_fetch_adding_to_another_folder_is_refused(tmp_path, app, job):
    folder, other = _library_folder(app), _library_folder(app)
    _fetch_into(job, folder)
    app.model.session.commit()
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_LIBRARIES, _add_to_folder(other.id))
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def test_other_tools_cannot_name_a_folder_through_their_parameters(tmp_path, app, job):
    folder = _library_folder(app)
    _fetch_into(job, folder, tool_id="not_the_data_fetch_tool")
    app.model.session.commit()
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_LIBRARIES, _add_to_folder(folder.id))
    with pytest.raises(MalformedContents):
        validate_job_import(_import_store(directory, app, job), job)


def test_the_job_exported_with_a_string_id_is_accepted(tmp_path, app, job):
    # Discovered outputs record their creating job again, with the id as a string.
    directory = _export_outputs(tmp_path, app, job)
    _rewrite(directory, store.ATTRS_FILENAME_JOBS, lambda jobs_attrs: jobs_attrs.append({"id": str(job.id)}))
    validate_job_import(_import_store(directory, app, job), job)
