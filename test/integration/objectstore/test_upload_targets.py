"""Clients upload straight to a boto3 object store through the targets it issues.

Runs against a disposable SeaweedFS container: each target is used the way a
remote job runner uses it, with plain HTTP and no storage credentials, and what
the store then holds is read back through Galaxy's object store.
"""

import hashlib
import os
from uuid import uuid4

import pytest
import requests

from galaxy.objectstore import (
    MultipartUploadTarget,
    PutUploadTarget,
)
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


@pytest.fixture
def object_store(seaweedfs):
    with TestConfig(DIRECT_UPLOAD_CONFIG, store_by="uuid") as (_, object_store):
        yield object_store


def _stored_bytes(object_store, obj, **location):
    with open(object_store.get_filename(obj, **location), "rb") as f:
        return f.read()


@skip_unless_docker()
def test_put_target_stores_the_upload(object_store):
    obj = StoredObject()
    contents = b"uploaded straight to the store\n"

    target = object_store.get_upload_target(obj, size=len(contents))

    assert isinstance(target, PutUploadTarget)
    requests.put(target.url, data=contents).raise_for_status()
    assert _stored_bytes(object_store, obj) == contents


@skip_unless_docker()
def test_put_target_refuses_an_upload_of_another_size(object_store):
    obj = StoredObject()
    contents = b"the size Galaxy signed\n"

    target = object_store.get_upload_target(obj, size=len(contents))

    assert isinstance(target, PutUploadTarget)
    response = requests.put(target.url, data=contents + b"and more\n")
    assert response.status_code == 403, response.text
    assert not object_store.exists(obj)


@skip_unless_docker()
def test_put_target_stores_a_file_with_its_object(object_store):
    obj = StoredObject()
    location = {"extra_dir": f"dataset_{obj.uuid}_files/sub", "alt_name": "part.txt"}
    contents = b"an extra file\n"

    target = object_store.get_upload_target(obj, size=len(contents), **location)

    assert isinstance(target, PutUploadTarget)
    requests.put(target.url, data=contents).raise_for_status()
    assert _stored_bytes(object_store, obj, **location) == contents


@skip_unless_docker()
def test_multipart_target_stores_the_upload_once_completed(object_store):
    obj = StoredObject()
    contents = os.urandom(11 * MiB)

    target = object_store.get_upload_target(obj, size=len(contents))

    assert isinstance(target, MultipartUploadTarget)
    assert len(target.parts) == 3
    completed = []
    for part in target.parts:
        response = requests.put(part["url"], data=contents[part["offset"] : part["offset"] + part["size"]])
        response.raise_for_status()
        completed.append({"part_number": part["part_number"], "etag": response.headers["ETag"]})
    object_store.complete_upload(obj, target.upload_id, completed)

    stored = _stored_bytes(object_store, obj)
    assert hashlib.sha256(stored).hexdigest() == hashlib.sha256(contents).hexdigest()
