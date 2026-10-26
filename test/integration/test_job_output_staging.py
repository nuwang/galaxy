"""The endpoints a remote job runner uses to upload a job's outputs.

Galaxy's store here is on disk and takes no direct uploads, so outputs are
uploaded to Galaxy, which writes them to the store.
"""

import os
import time
from urllib.parse import urljoin
from uuid import uuid4

import requests
from sqlalchemy import select

from galaxy import model
from galaxy.managers.job_output_staging import output_targets_url
from galaxy.model.base import ensure_object_added_to_session
from galaxy_test.base import api_asserts
from galaxy_test.base.populators import DatasetPopulator
from galaxy_test.driver import integration_util

SCRIPT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
SIMPLE_JOB_CONFIG_FILE = os.path.join(SCRIPT_DIRECTORY, "simple_job_conf.xml")
OUTPUT_CONTENT = b"uploaded output\n"


class TestJobOutputStagingIntegration(integration_util.IntegrationTestCase):
    dataset_populator: DatasetPopulator

    @classmethod
    def handle_galaxy_config_kwds(cls, config):
        super().handle_galaxy_config_kwds(config)
        config["job_config_file"] = SIMPLE_JOB_CONFIG_FILE
        config["object_store_store_by"] = "uuid"

    @property
    def sa_session(self):
        return self._app.model.session

    def setUp(self):
        super().setUp()
        self.dataset_populator = DatasetPopulator(self.galaxy_interactor)
        self.history_id = self.dataset_populator.new_history()

    def test_output_is_uploaded_through_galaxy(self):
        job, output = self._running_job_with_output()

        target = self._targets(job, [self._file(output.uuid)]).json()["targets"][0]
        response = requests.put(urljoin(self._targets_url(job), target["url"]), data=OUTPUT_CONTENT)

        api_asserts.assert_status_code_is(response, 204)
        with open(self._app.object_store.get_filename(output), "rb") as f:
            assert f.read() == OUTPUT_CONTENT

    def test_new_dataset_is_uploaded_under_its_unused_uuid(self):
        job, _ = self._running_job_with_output()
        new_uuid = uuid4()

        target = self._targets(job, [self._file(new_uuid)]).json()["targets"][0]
        response = requests.put(urljoin(self._targets_url(job), target["url"]), data=OUTPUT_CONTENT)

        api_asserts.assert_status_code_is(response, 204)
        assert self._app.object_store.exists(model.Dataset(uuid=new_uuid))

    def test_targets_for_another_datasets_uuid_are_refused(self):
        job, _ = self._running_job_with_output()
        _, other = self._running_job_with_output()
        api_asserts.assert_status_code_is(self._targets(job, [self._file(other.uuid)]), 403)

    def test_targets_request_with_a_tampered_signature_is_refused(self):
        job, output = self._running_job_with_output()
        url = self._targets_url(job).replace("sig=", "sig=0")
        response = requests.post(url, json={"files": [self._file(output.uuid)]})
        api_asserts.assert_status_code_is(response, 403)

    def test_upload_of_more_than_galaxy_accepted_is_refused(self):
        job, output = self._running_job_with_output()

        target = self._targets(job, [self._file(output.uuid)]).json()["targets"][0]
        response = requests.put(urljoin(self._targets_url(job), target["url"]), data=OUTPUT_CONTENT + b"more\n")

        api_asserts.assert_status_code_is(response, 400)
        assert not self._app.object_store.exists(output)

    def _file(self, uuid, size=len(OUTPUT_CONTENT)):
        return {"class": "Dataset", "uuid": str(uuid), "size": size}

    def _targets(self, job, files):
        return requests.post(self._targets_url(job), json={"files": files})

    def _targets_url(self, job):
        expires = int(time.time()) + 3600
        return output_targets_url(self._app.security, self.url.rstrip("/"), job.id, expires)

    def _running_job_with_output(self):
        """A job whose handler is unknown, so its state stays as set here, with a new output."""
        sa_session = self.sa_session
        history = sa_session.get(model.History, self._app.security.decode_id(self.history_id))
        assert history is not None
        output = model.HistoryDatasetAssociation(history=history, create_dataset=True, sa_session=sa_session)
        job = model.Job()
        job.history = history
        ensure_object_added_to_session(job, object_in_session=history)
        job.user = sa_session.scalars(select(model.User)).first()
        job.handler = "unknown-handler"
        job.state = model.Job.states.RUNNING
        job.add_output_dataset("output1", output)
        sa_session.add_all([output, job])
        sa_session.commit()
        assert output.dataset is not None
        return job, output.dataset
