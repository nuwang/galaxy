"""Clients upload straight to a boto3 or cloud object store through the targets it issues.

Runs against a disposable SeaweedFS container: each target is used the way a
remote job runner uses it, with plain HTTP and no storage credentials, and what
the store then holds is read back through Galaxy's object store.
"""

import hashlib
import os
from uuid import uuid4

import pytest
import requests

from galaxy.exceptions import RequestParameterInvalidException
from galaxy.objectstore import MultipartUploadTarget
from galaxy.objectstore.unittest_utils import Config as TestConfig
from galaxy_test.driver.integration_util import (
    docker_rm,
    skip_unless_docker,
)
from ._base import (
    OBJECT_STORE_ACCESS_KEY,
    OBJECT_STORE_HOST,
    OBJECT_STORE_PORT,
    OBJECT_STORE_SECRET_KEY,
    start_seaweedfs,
)

MiB = 1024 * 1024
DIRECT_UPLOAD_CONFIG = f"""
type: boto3
enable_direct_upload: true
auth:
  access_key: {OBJECT_STORE_ACCESS_KEY}
  secret_key: {OBJECT_STORE_SECRET_KEY}
bucket:
  name: galaxy
connection:
  endpoint_url: http://{OBJECT_STORE_HOST}:{OBJECT_STORE_PORT}
transfer:
  upload_multipart_threshold: {5 * MiB}
  upload_multipart_chunksize: {5 * MiB}
cache:
  path: ${{temp_directory}}/object_store_cache
  size: 1000
extra_dirs:
- type: job_work
  path: ${{temp_directory}}/job_working_directory
- type: temp
  path: ${{temp_directory}}/tmp
"""


class StoredObject:
    def __init__(self):
        self.id = 1
        self.uuid = uuid4()
        self.object_store_id = None


@pytest.fixture(scope="module")
def seaweedfs():
    container_name = f"test_upload_targets_{uuid4().hex[:8]}"
    # Signatures are checked only when the gateway requires credentials.
    start_seaweedfs(container_name, require_auth=True)
    yield
    docker_rm(container_name)


# The cloud store reaches SeaweedFS through cloudbridge's AWS provider; SigV4 signs each part's size.
CLOUD_DIRECT_UPLOAD_CONFIG = (
    DIRECT_UPLOAD_CONFIG.replace("type: boto3", "type: cloud\nprovider: aws")
    .replace("connection:\n", "connection:\n  signature_version: s3v4\n")
    .replace(f"  upload_multipart_threshold: {5 * MiB}\n", "")
)


@pytest.fixture(params=[DIRECT_UPLOAD_CONFIG, CLOUD_DIRECT_UPLOAD_CONFIG], ids=["boto3", "cloud"])
def object_store(seaweedfs, request):
    with TestConfig(request.param, store_by="uuid") as (_, object_store):
        yield object_store


def _stored_bytes(object_store, obj, **location):
    with open(object_store.get_filename(obj, **location), "rb") as f:
        return f.read()


def _upload(object_store, obj, contents, **location) -> list[str]:
    """Upload ``contents`` through the target the store issues, as a remote job runner does; return its URLs."""
    target = object_store.get_upload_target(obj, size=len(contents), **location)
    assert isinstance(target, MultipartUploadTarget)
    completed = []
    for part in target.parts:
        response = _put_part(part, contents[part["offset"] : part["offset"] + part["size"]])
        response.raise_for_status()
        completed.append({"part_number": part["part_number"], "etag": response.headers["ETag"]})
    object_store.complete_upload(obj, target.upload_id, completed, len(contents), **location)
    return [part["url"] for part in target.parts]


def _put_part(part, data):
    return requests.put(part["url"], data=data, headers=part.get("headers"))


@skip_unless_docker()
def test_upload_urls_cannot_replace_the_object_once_uploaded(object_store):
    # The URLs work until they expire, long after Galaxy has imported the output.
    obj = StoredObject()
    contents = b"what the job uploaded\n"
    urls = _upload(object_store, obj, contents)

    for url in urls:
        requests.put(url, data=b"what replaced it later\n"[: len(contents)])

    assert object_store.get_data_head(obj, 1024) == contents


@skip_unless_docker()
def test_small_object_is_uploaded_in_one_part(object_store):
    obj = StoredObject()
    contents = b"uploaded straight to the store\n"

    assert len(_upload(object_store, obj, contents)) == 1

    assert _stored_bytes(object_store, obj) == contents


@skip_unless_docker()
def test_part_url_refuses_an_upload_of_another_size(object_store):
    obj = StoredObject()
    contents = b"the size Galaxy signed\n"

    target = object_store.get_upload_target(obj, size=len(contents))

    assert isinstance(target, MultipartUploadTarget)
    response = _put_part(target.parts[0], contents + b"and more\n")
    assert response.status_code == 403, response.text
    assert not object_store.exists(obj)


@skip_unless_docker()
def test_upload_stores_a_file_with_its_object(object_store):
    obj = StoredObject()
    location = {"extra_dir": f"dataset_{obj.uuid}_files/sub", "alt_name": "part.txt"}
    contents = b"an extra file\n"

    _upload(object_store, obj, contents, **location)

    assert _stored_bytes(object_store, obj, **location) == contents


@skip_unless_docker()
def test_large_object_is_uploaded_in_parts(object_store):
    obj = StoredObject()
    contents = os.urandom(11 * MiB)

    assert len(_upload(object_store, obj, contents)) == 3

    stored = _stored_bytes(object_store, obj)
    assert hashlib.sha256(stored).hexdigest() == hashlib.sha256(contents).hexdigest()


@skip_unless_docker()
def test_part_reported_without_its_etag_is_refused(object_store):
    obj = StoredObject()
    contents = b"what the job uploaded\n"
    target = object_store.get_upload_target(obj, size=len(contents))
    assert isinstance(target, MultipartUploadTarget)
    _put_part(target.parts[0], contents).raise_for_status()

    with pytest.raises(RequestParameterInvalidException):
        object_store.complete_upload(obj, target.upload_id, [{"part_number": 1, "etag": None}], len(contents))
    assert not object_store.exists(obj)
