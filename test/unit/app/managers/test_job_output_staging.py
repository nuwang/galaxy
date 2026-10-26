import time
from unittest import mock
from urllib.parse import (
    parse_qs,
    urlparse,
)
from uuid import uuid4

import pytest

from galaxy import exceptions
from galaxy.managers.job_output_staging import (
    JobOutputStagingManager,
    output_targets_url,
    StagedOutputFile,
)
from galaxy.model import (
    Dataset,
    HistoryDatasetAssociation,
    Job,
    MetadataFile,
)
from galaxy.model.unittest_utils import (
    GalaxyDataTestApp,
    GalaxyDataTestConfig,
)
from galaxy.objectstore import (
    MultipartUploadTarget,
    PutUploadTarget,
    UploadPart,
)

GALAXY_URL = "https://galaxy.test"
CONTENT = b"output content\n"
LATER = int(time.time()) + 3600


@pytest.fixture
def max_discovered_files():
    return 10


@pytest.fixture
def app(max_discovered_files):
    return GalaxyDataTestApp(GalaxyDataTestConfig(max_discovered_files=max_discovered_files))


@pytest.fixture
def manager(app) -> JobOutputStagingManager:
    return JobOutputStagingManager(app.model.session, app.object_store, app.security, app.config)


def _hda(app) -> HistoryDatasetAssociation:
    hda = HistoryDatasetAssociation(sa_session=app.model.session, create_dataset=True)
    app.model.session.add(hda)
    app.model.session.commit()
    return hda


def _dataset(hda: HistoryDatasetAssociation) -> Dataset:
    assert hda.dataset is not None
    return hda.dataset


def _running_job(app, *outputs) -> Job:
    job = Job()
    job.state = Job.states.RUNNING
    for i, hda in enumerate(outputs):
        job.add_output_dataset(f"output{i}", hda)
    app.model.session.add(job)
    app.model.session.commit()
    return job


def _targets_request(app, job, expires=LATER):
    """What Pulsar's targets request carries, read back from the URL Galaxy issues."""
    query = parse_qs(urlparse(output_targets_url(app.security, GALAXY_URL, job.id, expires)).query)
    return {"job_id": job.id, "expires": int(query["exp"][0]), "signature": query["sig"][0]}


def _galaxy_upload_request(job, url):
    """What Pulsar sends to an upload URL Galaxy answers itself, relative to the targets URL."""
    parsed = urlparse(url)
    assert not parsed.scheme and not parsed.netloc
    query = parse_qs(parsed.query)
    return {
        "job_id": job.id,
        "target": query["target"][0],
        "expires": int(query["exp"][0]),
        "signature": query["sig"][0],
    }


def _file(uuid, size=len(CONTENT), **fields) -> StagedOutputFile:
    return StagedOutputFile.model_validate({"class": "Dataset", "uuid": str(uuid), "size": size, **fields})


def _metadata_file(uuid, dataset_uuid, size=len(CONTENT), **fields) -> StagedOutputFile:
    location = {"extra_dir": "_metadata_files", "extra_dir_at_root": True, "alt_name": f"metadata_{uuid}.dat"}
    return StagedOutputFile.model_validate(
        {
            "class": "MetadataFile",
            "uuid": str(uuid),
            "dataset_uuid": str(dataset_uuid),
            "size": size,
            **location,
            **fields,
        }
    )


def _upload_through_galaxy(manager, job, target, content=CONTENT):
    assert target["type"] == "put"
    upload = manager.start_object_upload(**_galaxy_upload_request(job, target["url"]))
    with open(upload.path, "wb") as f:
        f.write(content)
    manager.finish_object_upload(upload)


def _stored(app, obj, **location):
    with open(app.object_store.get_filename(obj, **location), "rb") as f:
        return f.read()


def test_output_targets_url_addresses_the_targets_endpoint(app):
    job = _running_job(app, _hda(app))
    url = urlparse(output_targets_url(app.security, GALAXY_URL, job.id, LATER))
    assert url.path == f"/api/jobs/{app.security.encode_id(job.id)}/staging/outputs/targets"


def test_output_is_uploaded_through_galaxy_when_the_store_takes_no_direct_uploads(app, manager):
    output = _hda(app)
    job = _running_job(app, output)

    [target] = manager.issue_targets(files=[_file(_dataset(output).uuid)], **_targets_request(app, job))
    _upload_through_galaxy(manager, job, target)

    assert _stored(app, _dataset(output)) == CONTENT


def test_presigned_put_target_is_passed_on(app, manager):
    output = _hda(app)
    job = _running_job(app, output)
    put = PutUploadTarget(url="https://bucket.example/presigned")
    with mock.patch.object(app.object_store, "get_upload_target", return_value=put) as get_upload_target:
        [target] = manager.issue_targets(files=[_file(_dataset(output).uuid)], **_targets_request(app, job))

    assert target == {"type": "put", "url": "https://bucket.example/presigned"}
    (obj, size), location = get_upload_target.call_args
    assert (obj.uuid, size, location) == (_dataset(output).uuid, len(CONTENT), {})


def test_multipart_target_is_completed_through_galaxy(app, manager):
    output = _hda(app)
    job = _running_job(app, output)
    parts = [
        UploadPart(part_number=1, url="https://bucket.example/1", offset=0, size=10),
        UploadPart(part_number=2, url="https://bucket.example/2", offset=10, size=5),
    ]
    multipart = MultipartUploadTarget(upload_id="upload-1", parts=parts)
    with mock.patch.object(app.object_store, "get_upload_target", return_value=multipart):
        [target] = manager.issue_targets(files=[_file(_dataset(output).uuid, size=15)], **_targets_request(app, job))
    assert target["type"] == "multipart"
    assert target["parts"] == parts

    completed = [{"part_number": 1, "etag": '"a"'}, {"part_number": 2, "etag": '"b"'}]
    with mock.patch.object(app.object_store, "complete_upload") as complete_upload:
        manager.complete_upload(parts=completed, **_galaxy_upload_request(job, target["complete_url"]))

    (obj, upload_id, uploaded_parts), location = complete_upload.call_args
    assert (obj.uuid, upload_id, uploaded_parts, location) == (_dataset(output).uuid, "upload-1", completed, {})


def test_new_dataset_with_an_unused_uuid_is_uploaded(app, manager):
    job = _running_job(app, _hda(app))
    new_uuid = uuid4()

    [target] = manager.issue_targets(files=[_file(new_uuid)], **_targets_request(app, job))
    _upload_through_galaxy(manager, job, target)

    assert _stored(app, Dataset(uuid=new_uuid)) == CONTENT


def test_new_dataset_taking_the_uuid_of_another_dataset_is_refused(app, manager):
    job = _running_job(app, _hda(app))
    other = _hda(app)
    with pytest.raises(exceptions.ItemAccessibilityException):
        manager.issue_targets(files=[_file(_dataset(other).uuid)], **_targets_request(app, job))


def test_new_dataset_in_a_store_the_job_was_not_given_is_refused(app, manager):
    job = _running_job(app, _hda(app))
    with pytest.raises(exceptions.ItemAccessibilityException):
        manager.issue_targets(files=[_file(uuid4(), object_store_id="elsewhere")], **_targets_request(app, job))


@pytest.mark.parametrize("max_discovered_files", [1])
def test_more_new_datasets_than_a_job_may_discover_are_refused(app, manager):
    job = _running_job(app, _hda(app))
    with pytest.raises(exceptions.RequestParameterInvalidException):
        manager.issue_targets(files=[_file(uuid4()), _file(uuid4())], **_targets_request(app, job))


def test_extra_file_in_its_datasets_directory_is_uploaded(app, manager):
    output = _hda(app)
    job = _running_job(app, output)
    uuid = _dataset(output).uuid
    location = {"extra_dir": f"dataset_{uuid}_files/sub", "alt_name": "part.txt"}

    [target] = manager.issue_targets(files=[_file(uuid, **location)], **_targets_request(app, job))
    _upload_through_galaxy(manager, job, target)

    assert _stored(app, _dataset(output), **location) == CONTENT


@pytest.mark.parametrize(
    "location",
    [
        {"extra_dir": "dataset_{other}_files", "alt_name": "part.txt"},
        {"extra_dir": "dataset_{uuid}_files/../dataset_{other}_files", "alt_name": "part.txt"},
        {"extra_dir": "dataset_{uuid}_files_and_more", "alt_name": "part.txt"},
        {"extra_dir": "dataset_{uuid}_files", "alt_name": "sub/part.txt"},
        {"extra_dir": "dataset_{uuid}_files", "alt_name": ".."},
        {"extra_dir": "dataset_{uuid}_files"},
        {"alt_name": "dataset_{other}.dat"},
        {"extra_dir": "dataset_{uuid}_files", "extra_dir_at_root": True, "alt_name": "part.txt"},
    ],
)
def test_file_outside_its_datasets_own_location_is_refused(app, manager, location):
    output = _hda(app)
    job = _running_job(app, output)
    uuid, other = _dataset(output).uuid, uuid4()
    fields = {
        key: value.format(uuid=uuid, other=other) if isinstance(value, str) else value
        for key, value in location.items()
    }
    with pytest.raises(exceptions.RequestParameterInvalidException):
        manager.issue_targets(files=[_file(uuid, **fields)], **_targets_request(app, job))


def test_metadata_file_of_an_output_is_uploaded(app, manager):
    output = _hda(app)
    job = _running_job(app, output)
    metadata_file = MetadataFile(dataset=output, uuid=uuid4())

    [target] = manager.issue_targets(
        files=[_metadata_file(metadata_file.uuid, _dataset(output).uuid)], **_targets_request(app, job)
    )
    _upload_through_galaxy(manager, job, target)

    with open(metadata_file.get_file_name(), "rb") as f:
        assert f.read() == CONTENT


def test_metadata_file_of_a_new_dataset_is_uploaded(app, manager):
    job = _running_job(app, _hda(app))
    new_uuid, metadata_file_uuid = uuid4(), uuid4()

    targets = manager.issue_targets(
        files=[_file(new_uuid), _metadata_file(metadata_file_uuid, new_uuid)], **_targets_request(app, job)
    )

    assert len(targets) == 2


@pytest.mark.parametrize(
    "location",
    [
        {"alt_name": "metadata_{other}.dat"},
        {"extra_dir": "_metadata_files/sub"},
        {"extra_dir_at_root": False},
    ],
)
def test_metadata_file_outside_its_own_location_is_refused(app, manager, location):
    output = _hda(app)
    job = _running_job(app, output)
    fields = {key: value.format(other=uuid4()) if isinstance(value, str) else value for key, value in location.items()}
    with pytest.raises(exceptions.RequestParameterInvalidException):
        manager.issue_targets(
            files=[_metadata_file(uuid4(), _dataset(output).uuid, **fields)], **_targets_request(app, job)
        )


def test_metadata_file_naming_no_dataset_is_refused(app, manager):
    job = _running_job(app, _hda(app))
    metadata_file_uuid = uuid4()
    location = {
        "extra_dir": "_metadata_files",
        "extra_dir_at_root": True,
        "alt_name": f"metadata_{metadata_file_uuid}.dat",
    }
    file = StagedOutputFile.model_validate(
        {"class": "MetadataFile", "uuid": str(metadata_file_uuid), "size": len(CONTENT), **location}
    )
    with pytest.raises(exceptions.RequestParameterInvalidException):
        manager.issue_targets(files=[file], **_targets_request(app, job))


def test_metadata_file_of_a_dataset_that_is_not_an_output_is_refused(app, manager):
    job = _running_job(app, _hda(app))
    other = _hda(app)
    with pytest.raises(exceptions.ItemAccessibilityException):
        manager.issue_targets(files=[_metadata_file(uuid4(), _dataset(other).uuid)], **_targets_request(app, job))


def test_metadata_file_taking_another_datasets_metadata_file_uuid_is_refused(app, manager):
    output, other = _hda(app), _hda(app)
    job = _running_job(app, output)
    others_metadata_file = MetadataFile(dataset=other, uuid=uuid4())
    app.model.session.add(others_metadata_file)
    app.model.session.commit()
    with pytest.raises(exceptions.ItemAccessibilityException):
        manager.issue_targets(
            files=[_metadata_file(others_metadata_file.uuid, _dataset(output).uuid)], **_targets_request(app, job)
        )


def test_targets_request_signed_for_another_job_is_refused(app, manager):
    output = _hda(app)
    job, other_job = _running_job(app, output), _running_job(app, _hda(app))
    request = {**_targets_request(app, other_job), "job_id": job.id}
    with pytest.raises(exceptions.ItemAccessibilityException):
        manager.issue_targets(files=[_file(_dataset(output).uuid)], **request)


def test_expired_targets_request_is_refused(app, manager):
    output = _hda(app)
    job = _running_job(app, output)
    with pytest.raises(exceptions.ItemAccessibilityException):
        manager.issue_targets(
            files=[_file(_dataset(output).uuid)], **_targets_request(app, job, expires=int(time.time()) - 1)
        )


def test_targets_request_for_a_finished_job_is_refused(app, manager):
    output = _hda(app)
    job = _running_job(app, output)
    request = _targets_request(app, job)
    job.state = Job.states.OK
    app.model.session.commit()
    with pytest.raises(exceptions.ItemAccessibilityException):
        manager.issue_targets(files=[_file(_dataset(output).uuid)], **request)


def test_upload_through_galaxy_of_another_size_is_refused(app, manager):
    output = _hda(app)
    job = _running_job(app, output)
    [target] = manager.issue_targets(files=[_file(_dataset(output).uuid)], **_targets_request(app, job))

    with pytest.raises(exceptions.RequestParameterInvalidException):
        _upload_through_galaxy(manager, job, target, content=CONTENT + b"and more\n")
    assert not app.object_store.exists(_dataset(output))


def test_upload_url_altered_to_reach_another_object_is_refused(app, manager):
    output = _hda(app)
    job = _running_job(app, output)
    [target] = manager.issue_targets(files=[_file(_dataset(output).uuid)], **_targets_request(app, job))
    [other_target] = manager.issue_targets(files=[_file(uuid4())], **_targets_request(app, job))
    request = {
        **_galaxy_upload_request(job, target["url"]),
        "target": _galaxy_upload_request(job, other_target["url"])["target"],
    }
    with pytest.raises(exceptions.ItemAccessibilityException):
        manager.start_object_upload(**request)
