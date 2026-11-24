"""Pulsar uploads a job's outputs through Galaxy when the object store takes no direct uploads.

As in ``objectstore/test_pulsar_output_targets.py`` but with Galaxy's disk object
store: each output the job's metadata step stages is uploaded to a signed Galaxy
URL, and Galaxy writes it to the store.
"""

import json
import os

from galaxy_test.base.populators import DatasetPopulator
from galaxy_test.driver import integration_util
from .objectstore._purged_handling import purge_while_job_running

SCRIPT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
JOB_CONFIG_FILE = os.path.join(SCRIPT_DIRECTORY, "embedded_pulsar_output_staging_job_conf.yml")
OTHER_CONTENT = "another dataset's content\n"


class TestPulsarOutputTargetsThroughGalaxy(integration_util.IntegrationTestCase):
    dataset_populator: DatasetPopulator
    framework_tool_and_types = True

    @classmethod
    def handle_galaxy_config_kwds(cls, config):
        super().handle_galaxy_config_kwds(config)
        config["job_config_file"] = JOB_CONFIG_FILE
        config["object_store_store_by"] = "uuid"
        config["metadata_strategy"] = "extended"
        config["retry_metadata_internally"] = False
        config["cleanup_job"] = "never"
        config["galaxy_infrastructure_url"] = "http://localhost:$GALAXY_WEB_PORT"

    def setUp(self):
        super().setUp()
        self.dataset_populator = DatasetPopulator(self.galaxy_interactor)

    def test_output_is_uploaded_through_galaxy(self, history_id):
        hda = self.dataset_populator.new_dataset(history_id, content="staged output\n", wait=True)
        run = self.dataset_populator.run_tool("cat1", {"input1": {"src": "hda", "id": hda["id"]}}, history_id)
        self.dataset_populator.wait_for_job(run["jobs"][0]["id"], assert_ok=True)

        output = run["outputs"][0]
        assert self.dataset_populator.get_history_dataset_content(history_id, dataset=output) == "staged output\n"
        job = self._app.model.session.get(self._app.model.Job, self._app.security.decode_id(run["jobs"][0]["id"]))
        working_directory = self._app.object_store.get_filename(job, base_dir="job_work", dir_only=True, obj_dir=True)
        with open(os.path.join(working_directory, "metadata", "object_store_conf.json")) as f:
            assert json.load(f)["type"] == "job_local"

    def test_composite_output_is_uploaded_through_galaxy(self):
        self._run_tool_test("composite_output")

    def test_metadata_files_are_uploaded_through_galaxy(self):
        self._run_tool_test("metadata_bam")

    def test_discovered_outputs_are_uploaded_through_galaxy(self):
        self._run_tool_test("multi_output")

    def test_outputs_purged_while_the_job_runs_stay_purged(self):
        purge_while_job_running(self.dataset_populator)

    def test_tool_listing_another_dataset_for_upload_fails_and_leaves_it_alone(self, history_id):
        other = self.dataset_populator.new_dataset(history_id, content=OTHER_CONTENT, wait=True)
        other_uuid = self.dataset_populator.get_history_dataset_details(history_id, dataset=other)["uuid"]

        run = self.dataset_populator.run_tool("forged_output_manifest", {"uuid": other_uuid}, history_id)
        self.dataset_populator.wait_for_job(run["jobs"][0]["id"], assert_ok=False)

        assert self.dataset_populator.get_job_details(run["jobs"][0]["id"]).json()["state"] == "error"
        content = self.dataset_populator.get_history_dataset_content(history_id, dataset=other, wait=False)
        assert content == OTHER_CONTENT
