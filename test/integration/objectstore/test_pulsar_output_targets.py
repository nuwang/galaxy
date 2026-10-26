"""Pulsar uploads a job's outputs to the targets Galaxy issues (``object_store_staging``).

Jobs run through embedded Pulsar with ``remote_transfer`` staging and extended
metadata set remotely, against a boto3 object store backed by a disposable
SeaweedFS container that checks signatures. The job's metadata step stages its
outputs in the job directory through a ``job_local`` object store, so the job
holds no storage credentials; after the job Pulsar asks Galaxy for an upload
target per staged file and uploads it straight to the bucket.
"""

import json
import os
import string

import boto3
from botocore.client import Config

from galaxy_test.base.populators import DatasetPopulator
from galaxy_test.driver import integration_util
from galaxy_test.driver.integration_util import docker_rm
from ._base import (
    OBJECT_STORE_ACCESS_KEY,
    OBJECT_STORE_HOST,
    OBJECT_STORE_PORT,
    OBJECT_STORE_SECRET_KEY,
    start_seaweedfs,
)

MiB = 1024 * 1024

JOB_CONFIG_FILE = os.path.join(os.path.dirname(__file__), os.pardir, "embedded_pulsar_output_staging_job_conf.yml")

BOTO3_DIRECT_UPLOAD_CONFIG = string.Template("""
<object_store type="boto3" enable_direct_upload="true">
    <auth access_key="${access_key}" secret_key="${secret_key}" />
    <bucket name="galaxy" />
    <connection endpoint_url="http://${host}:${port}" />
    <cache path="${temp_directory}/object_store_cache" size="1000" />
    <transfer upload_multipart_threshold="${part_size}" upload_multipart_chunksize="${part_size}" />
    <extra_dir type="job_work" path="${temp_directory}/job_working_directory_boto3"/>
    <extra_dir type="temp" path="${temp_directory}/tmp_boto3"/>
</object_store>
""")


class PulsarOutputTargetsInstance(integration_util.IntegrationInstance):
    framework_tool_and_types = True
    container_name = "pulsar_output_targets_container"

    @classmethod
    def setUpClass(cls):
        start_seaweedfs(cls.container_name, require_auth=True)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        docker_rm(cls.container_name)

    @classmethod
    def handle_galaxy_config_kwds(cls, config):
        super().handle_galaxy_config_kwds(config)
        temp_directory = cls._test_driver.mkdtemp()
        object_store_config_path = os.path.join(temp_directory, "object_store_conf.xml")
        with open(object_store_config_path, "w") as f:
            f.write(
                BOTO3_DIRECT_UPLOAD_CONFIG.substitute(
                    temp_directory=temp_directory,
                    host=OBJECT_STORE_HOST,
                    port=OBJECT_STORE_PORT,
                    access_key=OBJECT_STORE_ACCESS_KEY,
                    secret_key=OBJECT_STORE_SECRET_KEY,
                    part_size=5 * MiB,
                )
            )
        config["object_store_config_file"] = object_store_config_path
        config["object_store_store_by"] = "uuid"
        config["metadata_strategy"] = "extended"
        config["retry_metadata_internally"] = False
        config["cleanup_job"] = "never"
        config["job_config_file"] = JOB_CONFIG_FILE
        config["galaxy_infrastructure_url"] = "http://localhost:$GALAXY_WEB_PORT"


instance = integration_util.integration_module_instance(PulsarOutputTargetsInstance)


def _bucket():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{OBJECT_STORE_HOST}:{OBJECT_STORE_PORT}",
        aws_access_key_id=OBJECT_STORE_ACCESS_KEY,
        aws_secret_access_key=OBJECT_STORE_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def _run_cat(instance, content):
    dataset_populator = DatasetPopulator(instance.galaxy_interactor)
    history_id = dataset_populator.new_history()
    hda = dataset_populator.new_dataset(history_id, content=content, wait=True)
    run = dataset_populator.run_tool("cat1", {"input1": {"src": "hda", "id": hda["id"]}}, history_id)
    dataset_populator.wait_for_job(run["jobs"][0]["id"], assert_ok=True)
    app = instance._app
    job = app.model.session.get(app.model.Job, app.security.decode_id(run["jobs"][0]["id"]))
    output = app.model.session.get(app.model.HistoryDatasetAssociation, app.security.decode_id(run["outputs"][0]["id"]))
    return job, output.dataset


def test_job_holds_no_storage_credentials(instance):
    job, dataset = _run_cat(instance, "staged output\n")

    working_directory = instance._app.object_store.get_filename(job, base_dir="job_work", dir_only=True, obj_dir=True)
    with open(os.path.join(working_directory, "metadata", "object_store_conf.json")) as f:
        object_store_conf = f.read()
    assert json.loads(object_store_conf)["type"] == "job_local"
    assert "secret_key" not in object_store_conf
    assert "access_key" not in object_store_conf
    key = instance._app.object_store._construct_path(dataset)
    assert _bucket().get_object(Bucket="galaxy", Key=key)["Body"].read() == b"staged output\n"


def test_large_output_is_uploaded_in_parts(instance):
    content = "a line of a large output\n" * (12 * MiB // 25)

    _, dataset = _run_cat(instance, content)

    key = instance._app.object_store._construct_path(dataset)
    stored = _bucket().head_object(Bucket="galaxy", Key=key)
    assert stored["ContentLength"] == len(content)
    # A multipart upload's ETag ends with the number of parts.
    assert stored["ETag"].strip('"').endswith("-3")


test_tools = integration_util.integration_tool_runner(
    [
        "simple_constructs",
        "metadata_bam",
        "composite_output",
        "multi_output",
        "multi_output_assign_primary",
        "collection_creates_dynamic_nested",
        "from_work_dir_glob",
    ]
)
