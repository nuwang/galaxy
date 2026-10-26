"""The job_local object store stages a job's outputs in its directory and lists them for upload."""

import errno
import json
import os
from unittest import mock

import pytest

from galaxy.exceptions import ConfigurationError
from galaxy.model import (
    Dataset,
    HistoryDatasetAssociation,
    MetadataFile,
)
from galaxy.objectstore import build_object_store_from_config
from galaxy.objectstore.job_local import (
    job_local_object_store_config,
    JobLocalObjectStore,
    MANIFEST_NAME,
)
from galaxy.objectstore.unittest_utils import Config as TestConfig
from galaxy.util.bunch import Bunch

APP_CONFIG = {
    "object_store_check_old_style": False,
    "enable_quotas": False,
    "file_path": "/galaxy/files",
    "umask": 0o077,
    "jobs_directory": "/galaxy/jobs",
    "new_file_path": "/galaxy/tmp",
    "object_store_cache_path": "/galaxy/cache",
    "object_store_cache_size": -1,
    "gid": None,
}


@pytest.fixture
def staging_directory(tmp_path):
    return tmp_path / "object_store_staging"


@pytest.fixture
def store(staging_directory, monkeypatch):
    store = build_object_store_from_config(
        None,
        config_dict={
            "type": "job_local",
            "files_dir": str(staging_directory),
            "store_by": "uuid",
            "config": APP_CONFIG,
        },
        disable_process_management=True,
    )
    monkeypatch.setattr(Dataset, "object_store", store)
    return store


@pytest.fixture
def source(tmp_path):
    def write(contents, name="output.dat"):
        path = tmp_path / "working" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        return str(path)

    return write


def _dataset(object_store_id="primary"):
    dataset = Dataset()
    dataset.object_store_id = object_store_id
    return dataset


def _manifest(staging_directory):
    manifest_path = staging_directory / MANIFEST_NAME
    if not manifest_path.exists():
        return []
    return [json.loads(line) for line in manifest_path.read_text().splitlines()]


def _staged(staging_directory, record):
    return (staging_directory / record["path"]).read_text()


def test_store_is_built_from_its_type(store):
    assert isinstance(store, JobLocalObjectStore)


def test_written_output_is_staged_and_listed(store, staging_directory, source):
    dataset = _dataset()
    store.update_from_file(dataset, file_name=source("output contents\n"), create=True)

    [record] = _manifest(staging_directory)
    assert record == {
        "path": record["path"],
        "class": "Dataset",
        "uuid": str(dataset.uuid),
        "object_store_id": "primary",
    }
    assert not os.path.isabs(record["path"])
    assert _staged(staging_directory, record) == "output contents\n"
    assert store.get_filename(dataset) == str(staging_directory / record["path"])


def test_staged_output_shares_the_written_file(store, staging_directory, source):
    output = source("large output\n")
    store.update_from_file(_dataset(), file_name=output, create=True)

    [record] = _manifest(staging_directory)
    assert os.stat(staging_directory / record["path"]).st_ino == os.stat(output).st_ino


def test_output_is_copied_when_it_cannot_be_linked(store, staging_directory, source, monkeypatch):
    def cross_device_link(src, dst, **kwargs):
        raise OSError(errno.EXDEV, "Invalid cross-device link")

    monkeypatch.setattr(os, "link", cross_device_link)
    output = source("copied output\n")
    store.update_from_file(_dataset(), file_name=output, create=True)

    [record] = _manifest(staging_directory)
    assert _staged(staging_directory, record) == "copied output\n"
    assert os.stat(staging_directory / record["path"]).st_ino != os.stat(output).st_ino


def test_symlinked_output_is_staged_as_its_contents(store, staging_directory, source, tmp_path):
    link = tmp_path / "working" / "link.dat"
    os.symlink(source("linked contents\n"), link)
    store.update_from_file(_dataset(), file_name=str(link), create=True, preserve_symlinks=True)

    [record] = _manifest(staging_directory)
    assert not os.path.islink(staging_directory / record["path"])
    assert _staged(staging_directory, record) == "linked contents\n"


def test_extra_file_is_listed_with_its_location(store, staging_directory, source):
    dataset = _dataset()
    extra_dir = f"dataset_{dataset.uuid}_files/sub"
    store.update_from_file(
        dataset, extra_dir=extra_dir, alt_name="part.txt", file_name=source("part\n", "part.txt"), create=True
    )

    [record] = _manifest(staging_directory)
    assert record == {
        "path": record["path"],
        "class": "Dataset",
        "uuid": str(dataset.uuid),
        "object_store_id": "primary",
        "extra_dir": extra_dir,
        "alt_name": "part.txt",
    }
    assert _staged(staging_directory, record) == "part\n"


def test_metadata_file_is_listed_with_its_dataset(store, staging_directory):
    dataset = _dataset()
    metadata_file = MetadataFile(dataset=HistoryDatasetAssociation(dataset=dataset), name="bam_index")
    # Datatypes write metadata files in place, then the metadata script pushes them.
    path = metadata_file.get_file_name()
    with open(path, "w") as f:
        f.write("index\n")
    metadata_file.update_from_file(path)

    [record] = _manifest(staging_directory)
    assert record == {
        "path": record["path"],
        "class": "MetadataFile",
        "uuid": str(metadata_file.uuid),
        "object_store_id": "primary",
        "extra_dir": "_metadata_files",
        "extra_dir_at_root": True,
        "alt_name": f"metadata_{metadata_file.uuid}.dat",
        "dataset_uuid": str(dataset.uuid),
    }
    assert _staged(staging_directory, record) == "index\n"


def test_object_is_listed_once(store, staging_directory, source):
    dataset = _dataset()
    store.create(dataset)
    store.update_from_file(dataset, file_name=source("first\n"), create=True)
    store.update_from_file(dataset, file_name=source("second\n", "second.dat"), create=True)

    [record] = _manifest(staging_directory)
    assert _staged(staging_directory, record) == "second\n"


def test_created_empty_object_is_listed(store, staging_directory):
    dataset = _dataset()
    store.create(dataset)

    [record] = _manifest(staging_directory)
    assert record["uuid"] == str(dataset.uuid)
    assert _staged(staging_directory, record) == ""


def test_directory_is_not_listed(store, staging_directory):
    dataset = _dataset()
    store.create(dataset, dir_only=True, extra_dir=f"dataset_{dataset.uuid}_files")

    assert _manifest(staging_directory) == []


def test_deleted_object_is_unlisted(store, staging_directory, source):
    kept, deleted = _dataset(), _dataset()
    store.update_from_file(kept, file_name=source("kept\n"), create=True)
    store.update_from_file(deleted, file_name=source("deleted\n", "deleted.dat"), create=True)

    assert store.delete(deleted)

    assert [record["uuid"] for record in _manifest(staging_directory)] == [str(kept.uuid)]


def test_deleting_an_empty_directory_leaves_the_manifest_alone(store, staging_directory, source):
    def create_and_delete_directory(dataset):
        location = {"extra_dir": f"dataset_{dataset.uuid}_files", "dir_only": True}
        store.create(dataset, **location)
        assert store.delete(dataset, entire_dir=True, **location)

    create_and_delete_directory(_dataset())
    assert not (staging_directory / MANIFEST_NAME).exists()

    staged = _dataset()
    store.update_from_file(staged, file_name=source("staged\n"), create=True)
    create_and_delete_directory(_dataset())

    assert [record["uuid"] for record in _manifest(staging_directory)] == [str(staged.uuid)]


def test_deleted_extra_files_directory_is_unlisted(store, staging_directory, source):
    dataset = _dataset()
    extra_dir = f"dataset_{dataset.uuid}_files"
    store.update_from_file(dataset, file_name=source("primary\n"), create=True)
    for name in ("a.txt", "b.txt"):
        store.update_from_file(dataset, extra_dir=extra_dir, alt_name=name, file_name=source(name, name), create=True)

    assert store.delete(dataset, entire_dir=True, extra_dir=extra_dir, dir_only=True)

    assert [record.get("alt_name") for record in _manifest(staging_directory)] == [None]


def test_store_is_rebuilt_from_its_config(store, staging_directory, source):
    rebuilt = build_object_store_from_config(None, config_dict=store.to_dict(), disable_process_management=True)
    dataset = _dataset()
    rebuilt.update_from_file(dataset, file_name=source("rebuilt\n"), create=True)

    assert isinstance(rebuilt, JobLocalObjectStore)
    [record] = _manifest(staging_directory)
    assert record["uuid"] == str(dataset.uuid)


def test_store_refuses_to_lay_out_objects_by_id(staging_directory):
    with pytest.raises(ConfigurationError):
        JobLocalObjectStore(Bunch(**APP_CONFIG), {"files_dir": str(staging_directory), "store_by": "id"})


BOTO3_CONFIG = """
type: boto3
auth:
  access_key: access_moo
  secret_key: secret_cow
bucket:
  name: unique_bucket_name_all_lowercase
cache:
  path: ${temp_directory}/object_store_cache
  size: 1000
"""


def test_job_config_stages_into_the_jobs_directory_without_credentials(staging_directory, source):
    # Galaxy's own store, with credentials; it is never contacted here.
    with mock.patch("galaxy.objectstore.s3_boto3.S3ObjectStore._initialize"):
        with TestConfig(BOTO3_CONFIG, store_by="uuid") as (_, galaxy_object_store):
            config = job_local_object_store_config(galaxy_object_store, str(staging_directory))

    assert "secret_cow" not in json.dumps(config) and "access_moo" not in json.dumps(config)
    job_store = build_object_store_from_config(None, config_dict=config, disable_process_management=True)
    dataset = _dataset()
    job_store.update_from_file(dataset, file_name=source("staged\n"), create=True)
    [record] = _manifest(staging_directory)
    assert record["uuid"] == str(dataset.uuid)
