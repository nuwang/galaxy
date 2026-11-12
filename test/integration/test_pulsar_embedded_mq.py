"""Integration tests for the Pulsar embedded runner with outputs to working directory."""

import os
import string
import tempfile
import uuid

import pytest

from galaxy.util import safe_makedirs
from galaxy_test.base.populators import DatasetPopulator
from galaxy_test.driver import integration_util
from .objectstore._purged_handling import purge_while_job_running

SCRIPT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
EMBEDDED_PULSAR_JOB_CONFIG_FILE = os.path.join(SCRIPT_DIRECTORY, "embedded_pulsar_mq_job_conf.yml")
AMQP_URL = os.environ.get("GALAXY_TEST_AMQP_URL", "amqp://guest:guest@localhost:5672//")

JOB_CONF_TEMPLATE = """
runners:
  local:
    load: galaxy.jobs.runners.local:LocalJobRunner
    workers: 1
  pulsar:
    load: galaxy.jobs.runners.pulsar:PulsarEmbeddedMQJobRunner
    pulsar_app_config:
      tool_dependency_dir: none
      conda_auto_init: false
      conda_auto_install: false
      message_queue_url: ${amqp_url}
      amqp_key_prefix: ${amqp_key_prefix}
      staging_directory: ${jobs_directory}
    amqp_url: ${amqp_url}
    amqp_key_prefix: ${amqp_key_prefix}
execution:
  default: pulsar_mq_environment
  environments:
    pulsar_mq_environment:
      runner: pulsar
      rewrite_parameters: true
      dependency_resolution: none
      default_file_action: remote_transfer
      jobs_directory: ${jobs_directory}
      remote_metadata: true
      remote_property_galaxy_home: ${galaxy_home}
${extra_environment_params}
    local_environment:
      runner: local
tools:
  - id: __DATA_FETCH__
    environment: local_environment
"""


def _handle_galaxy_config_kwds(cls, config, object_store_staging=None):
    amqp_url = os.environ.get("GALAXY_TEST_AMQP_URL", None)
    if amqp_url is None:
        pytest.skip("External AMQP URL not configured for test")

    jobs_directory = os.path.join(cls._test_driver.mkdtemp(), "pulsar_staging")
    safe_makedirs(jobs_directory)
    job_conf_template = string.Template(JOB_CONF_TEMPLATE)
    job_conf_str = job_conf_template.substitute(
        amqp_url=AMQP_URL,
        # Queues per instance, so a still-running module-level instance cannot consume the
        # job messages of another class.
        amqp_key_prefix=f"pulsar_{uuid.uuid4().hex[:12]}_",
        jobs_directory=jobs_directory,
        galaxy_home=os.path.join(SCRIPT_DIRECTORY, os.pardir),
        extra_environment_params=f"      object_store_staging: {object_store_staging}" if object_store_staging else "",
    )
    with tempfile.NamedTemporaryFile(suffix="_mq_job_conf.yml", mode="w", delete=False) as job_conf:
        job_conf.write(job_conf_str)
    config["job_config_file"] = job_conf.name
    infrastructure_url = "http://localhost:$GALAXY_WEB_PORT"
    config["galaxy_infrastructure_url"] = infrastructure_url


class TestEmbeddedMessageQueuePulsarPurge(integration_util.IntegrationTestCase):
    dataset_populator: DatasetPopulator
    framework_tool_and_types = True

    def setUp(self) -> None:
        super().setUp()
        self.dataset_populator = DatasetPopulator(self.galaxy_interactor)

    @classmethod
    def handle_galaxy_config_kwds(cls, config):
        _handle_galaxy_config_kwds(cls, config)

    def test_purge_while_job_running(self):
        purge_while_job_running(self.dataset_populator)


class TestEmbeddedMessageQueuePulsarExtendedMetadataPurge(TestEmbeddedMessageQueuePulsarPurge):
    """Describe a Galaxy test instance with embedded pulsar and extended metadata configured.

    $ Setup RabbitMQ (e.g. https://www.rabbitmq.com/install-homebrew.html)
    $ GALAXY_TEST_AMQP_URL='amqp://guest:guest@localhost:5672//' pytest -s test/integration/test_pulsar_embedded_mq.py
    """

    @classmethod
    def handle_galaxy_config_kwds(cls, config):
        config["metadata_strategy"] = "extended"
        _handle_galaxy_config_kwds(cls, config)


class TestEmbeddedMessageQueuePulsarOutputStaging(integration_util.IntegrationTestCase):
    """Outputs leave the job through upload targets Galaxy issues (object_store_staging).

    $ GALAXY_TEST_AMQP_URL='amqp://guest:guest@localhost:5672//' pytest -s test/integration/test_pulsar_embedded_mq.py
    """

    dataset_populator: DatasetPopulator
    framework_tool_and_types = True

    def setUp(self) -> None:
        super().setUp()
        self.dataset_populator = DatasetPopulator(self.galaxy_interactor)

    @classmethod
    def handle_galaxy_config_kwds(cls, config):
        config["metadata_strategy"] = "extended"
        config["object_store_store_by"] = "uuid"
        config["retry_metadata_internally"] = False
        _handle_galaxy_config_kwds(cls, config, object_store_staging="stream")

    def test_output_is_uploaded_to_its_target(self, history_id):
        hda = self.dataset_populator.new_dataset(history_id, content="staged output\n", wait=True)
        run = self.dataset_populator.run_tool("cat1", {"input1": {"src": "hda", "id": hda["id"]}}, history_id)
        self.dataset_populator.wait_for_job(run["jobs"][0]["id"], assert_ok=True)
        output = run["outputs"][0]
        assert self.dataset_populator.get_history_dataset_content(history_id, dataset=output) == "staged output\n"

    def test_composite_output_is_uploaded_to_its_targets(self):
        self._run_tool_test("composite_output")

    def test_tool_listing_another_dataset_for_upload_fails_and_leaves_it_alone(self, history_id):
        other = self.dataset_populator.new_dataset(history_id, content="another dataset's content\n", wait=True)
        other_uuid = self.dataset_populator.get_history_dataset_details(history_id, dataset=other)["uuid"]

        run = self.dataset_populator.run_tool("forged_output_manifest", {"uuid": other_uuid}, history_id)
        self.dataset_populator.wait_for_job(run["jobs"][0]["id"], assert_ok=False)

        assert self.dataset_populator.get_job_details(run["jobs"][0]["id"]).json()["state"] == "error"
        content = self.dataset_populator.get_history_dataset_content(history_id, dataset=other, wait=False)
        assert content == "another dataset's content\n"


class EmbeddedMessageQueuePulsarIntegrationInstance(integration_util.IntegrationInstance):
    """Describe a Galaxy test instance with embedded pulsar configured.

    $ Setup RabbitMQ (e.g. https://www.rabbitmq.com/install-homebrew.html)
    $ GALAXY_TEST_AMQP_URL='amqp://guest:guest@localhost:5672//' pytest -s test/integration/test_pulsar_embedded_mq.py
    """

    dataset_populator: DatasetPopulator
    framework_tool_and_types = True

    @classmethod
    def handle_galaxy_config_kwds(cls, config):
        _handle_galaxy_config_kwds(cls, config)

    def test_purge_while_job_running(self):
        purge_while_job_running(self.dataset_populator)


instance = integration_util.integration_module_instance(EmbeddedMessageQueuePulsarIntegrationInstance)

test_tools = integration_util.integration_tool_runner(
    ["simple_constructs", "composite_output_tests", "all_output_types"]
)
