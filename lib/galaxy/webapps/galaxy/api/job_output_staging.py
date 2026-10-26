"""API a remote job runner uses to upload a running job's outputs to the object store.

Not part of Galaxy's user-facing API: Galaxy issues these URLs to job runners
(see ``galaxy.managers.job_output_staging``) and a URL's signature, not a user
session, authorizes the request.
"""

import anyio
from fastapi import (
    Query,
    Request,
)
from fastapi.responses import Response
from starlette.requests import ClientDisconnect

from galaxy import exceptions
from galaxy.managers.job_output_staging import (
    CompleteUploadRequest,
    JobOutputStagingManager,
    ObjectUpload,
    OutputTargetsRequest,
    OutputTargetsResponse,
)
from galaxy.objectstore import CompletedPart
from galaxy.schema.fields import DecodedDatabaseIdField
from galaxy.webapps.base.api import release_request_sessions
from galaxy.webapps.galaxy.api import (
    depends,
    Router,
)

router = Router(tags=["remote files"])

OUTPUTS_PATH = "/api/jobs/{job_id}/staging/outputs"
EXPIRES = Query(description="Expiry of the staging URL, in seconds since the epoch.")
SIGNATURE = Query(description="Signature Galaxy issued for this job, upload and expiry.")
TARGET = Query(description="The upload Galaxy accepted, as Galaxy encoded it.")


@router.cbv
class FastAPIJobOutputStaging:
    manager: JobOutputStagingManager = depends(JobOutputStagingManager)

    @router.post(
        f"{OUTPUTS_PATH}/targets", summary="Issue upload targets for a job's outputs.", include_in_schema=False
    )
    def issue_targets(
        self,
        job_id: DecodedDatabaseIdField,
        payload: OutputTargetsRequest,
        exp: int = EXPIRES,
        sig: str = SIGNATURE,
    ) -> OutputTargetsResponse:
        return OutputTargetsResponse(targets=self.manager.issue_targets(job_id, exp, sig, payload.files))

    @router.post(
        f"{OUTPUTS_PATH}/complete", summary="Complete a multipart upload of an output.", include_in_schema=False
    )
    def complete_upload(
        self,
        job_id: DecodedDatabaseIdField,
        payload: CompleteUploadRequest,
        target: str = TARGET,
        exp: int = EXPIRES,
        sig: str = SIGNATURE,
    ) -> Response:
        parts = [CompletedPart(part_number=part.part_number, etag=part.etag) for part in payload.parts]
        self.manager.complete_upload(job_id, target, exp, sig, parts)
        return Response(status_code=204)

    @router.put(f"{OUTPUTS_PATH}/object", summary="Upload an output through Galaxy.", include_in_schema=False)
    async def upload_object(
        self,
        request: Request,
        job_id: DecodedDatabaseIdField,
        target: str = TARGET,
        exp: int = EXPIRES,
        sig: str = SIGNATURE,
    ) -> Response:
        upload = await anyio.to_thread.run_sync(self.manager.start_object_upload, job_id, target, exp, sig)
        try:
            release_request_sessions()
            await _receive(request, upload)
            await anyio.to_thread.run_sync(self.manager.finish_object_upload, upload)
        finally:
            await anyio.to_thread.run_sync(upload.discard)
        return Response(status_code=204)


async def _receive(request: Request, upload: ObjectUpload) -> None:
    """Write the request body to the upload's file, refusing more bytes than Galaxy accepted."""
    received = 0
    with open(upload.path, "wb") as f:
        try:
            async for chunk in request.stream():
                received += len(chunk)
                if received > upload.size:
                    raise exceptions.RequestParameterInvalidException(
                        f"Upload refused: more than the {upload.size} bytes Galaxy accepted."
                    )
                await anyio.to_thread.run_sync(f.write, chunk)
        except ClientDisconnect:
            raise exceptions.RequestParameterInvalidException("Client disconnected during output upload.")
