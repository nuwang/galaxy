from galaxy.jobs.job_destination import JobDestination

DEFAULT_INITIAL_ENVIRONMENT = "fail_first_try"


def initial_target_environment(resource_params):
    return resource_params.get("initial_target_environment", None) or DEFAULT_INITIAL_ENVIRONMENT


def dynamic_resubmit_once(resource_params) -> JobDestination:
    """Build environment that always fails first time and always re-routes to passing environment."""
    return JobDestination(
        # Always fail on the first attempt.
        runner="failure_runner",
        # Resubmit to a valid environment.
        resubmit=[
            dict(
                condition="any_failure",
                environment="local",
            )
        ],
    )


def dynamic_resubmit_with_more_memory(job) -> JobDestination:
    """Build a dynamic local destination that doubles memory on each OOM resubmission."""
    previous_params = job.destination_params or {}
    previous_scaling_factor = int(previous_params.get("SCALING_FACTOR", 0))
    scaling_factor = previous_scaling_factor * 2 if previous_scaling_factor else 1
    memory_mb = scaling_factor * 4
    return JobDestination(
        id=f"dynamic_memory_{memory_mb}",
        runner="local",
        env=[
            {
                "name": "GALAXY_MEMORY_MB",
                "value": str(memory_mb),
            }
        ],
        params={
            "SCALING_FACTOR": str(scaling_factor),
        },
        resubmit=[
            dict(
                condition="memory_limit_reached and attempt <= 3",
                destination="initial_destination",
            )
        ],
    )
