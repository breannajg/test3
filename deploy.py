"""CLI application to deploy DSCoE Databricks jobs."""
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import httpx
import typer
from typer import echo

TERMINAL_RUN_LIFECYCLE_STATES = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]
DEFAULT_DEPLOY_FILE_PATH = "conf/deployment.json"


class ApiClient(object):
    """An implementation of the Databricks Jobs and Repos APIs."""

    def __init__(
        self,
        host: str,
        token: str,
        repos_api_version: str = "2.0",
        jobs_api_version: str = "2.1",
    ) -> None:
        headers = {"Authorization": f"Bearer {token}"}
        repos_base_url = f"{host}/api/{repos_api_version}/repos"
        jobs_base_url = f"{host}/api/{jobs_api_version}/jobs"

        self.jobs_client = httpx.Client(base_url=jobs_base_url, headers=headers)
        self.repos_client = httpx.Client(base_url=repos_base_url, headers=headers)

    def close(self):
        """Close httpx clients."""
        self.jobs_client.close()
        self.repos_client.close()

    def _get_job_status(self, run_id: Dict[str, int]) -> Dict[str, Any]:
        get_job_status = self.jobs_client.get("/runs/get", params=run_id)
        get_job_status.raise_for_status()
        job_status: Dict[str, Any] = get_job_status.json()
        return job_status

    def _get_jobs(self) -> List[Dict[str, Any]]:
        limit = 25
        offset = 0
        jobs: List[Dict[str, Any]] = []
        has_more = True

        while has_more is True:
            r = self.jobs_client.get("/list", params={"limit": limit, "offset": offset})
            r.raise_for_status()
            result: Dict[str, Any] = r.json()
            jobs.extend(result["jobs"])
            limit += 25
            offset += 25
            has_more = result["has_more"]

        return jobs

    def get_job(self, job_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve job settings if job exists, else return None.

        Args:
            job_name: Name of job to search for
        """
        all_jobs = self._get_jobs()
        matching_job = [j for j in all_jobs if j["settings"]["name"] == job_name]

        if len(matching_job) > 1:
            raise Exception(
                f"""There are duplicate jobs with the name {job_name}.
            Please delete the duplicates."""
            )

        if matching_job:
            return matching_job[0]
        else:
            return None

    def submit_one_time_run(self, job_def: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a one-time run of a job and trace its status.

        Args:
            job_def: Dictionary defining the specs of the job,
            matching the schema defined at:
            https://redocly.github.io/redoc/?url=https://docs.microsoft.com/azure/databricks/_static/api-refs/jobs-2.1-azure.yaml#operation/JobsRunsSubmit
        """
        run_submit_request = self.jobs_client.post("/runs/submit", json=job_def)
        run_submit_request.raise_for_status()
        run_id: Dict[str, int] = run_submit_request.json()
        job_status = self._get_job_status(run_id)
        echo(f"Run URL: {job_status['run_page_url']}")
        while True:
            time.sleep(5)
            job_status = self._get_job_status(run_id)
            lifecycle_state: str = job_status["state"]["life_cycle_state"]
            msg = [
                f"[Run Id: {run_id['run_id']}] Current run status info - ",
                json.dumps(job_status, indent=2),
            ]
            echo("\n".join(msg))
            if lifecycle_state in TERMINAL_RUN_LIFECYCLE_STATES:
                echo(f"Finished tracing run with id {run_id['run_id']}")
                return job_status

    def create_job(self, job_def: Dict[str, Any]) -> Dict[str, int]:
        """Create new job in Databricks.

        Args:
            job_def: Dictionary holding job definition.
        """
        echo(f"Creating a new job with name {job_def['name']}")
        r = self.jobs_client.post("/create", json=job_def)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            echo("Failed to create job with definition:")
            echo(json.dumps(job_def, indent=2))
            echo(r.text)
            raise e

        job_id: Dict[str, int] = r.json()
        return job_id

    def update_job(self, job_id: int, job_def: Dict[str, Any]) -> Dict[str, Any]:
        """Selectively update job definition in Databricks.

        Args:
            job_id: ID of job to update.
            job_def: Dictionary holding job definition values to be updated.
        """
        echo(f"Updating job with id: {job_id}")
        body = {
            "job_id": job_id,
            "new_settings": job_def,
        }
        try:
            r = self.jobs_client.post("/update", json=body)
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            echo(f"Failed to update job id [{job_id}] with definition:")
            echo(json.dumps(job_def, indent=2))
            raise e

        update_result: Dict[str, Any] = r.json()
        return update_result


def deploy(
    deploy_name: str,
    job_definition_file: Path = typer.Option(DEFAULT_DEPLOY_FILE_PATH),
    as_run_submit: bool = False,
    host: str = typer.Option(None, envvar="DATABRICKS_HOST"),
    token: str = typer.Option(None, envvar="DATABRICKS_TOKEN"),
):
    """Deploy the given job definition into the Databricks workspace.

    If the job doesn't exist, it is created. If it does, it is updated.

    Args:
        deploy_name: Name of the deployment config within the file.
        job_definition_file: Path to job deployment config, based on API v2.1
        as_run_submit: Submit a one-time run instead of creating a new job
        host: Databricks host URI
        token: Databricks Personal Access Token
    """
    with open(job_definition_file) as f:
        job_defs: Dict[str, Dict[str, Any]] = json.load(f)
    job_def = job_defs[deploy_name]

    if host[:4] != "http":
        host = f"https://{host}"
    if host[-1] == "/":
        host = host[:-1]
    client = ApiClient(host, token)

    if as_run_submit:
        typer.echo(f"Submitting run-time run of {deploy_name}")
        final_state = client.submit_one_time_run(job_def)
        result = final_state["state"].get("result_state", None)
        if result == "SUCCESS":
            typer.echo("Job run finished successfully")
        elif result == "ERROR":
            raise Exception(
                "Tracked run failed during execution. "
                "Please check Databricks UI for run logs"
            )
        client.close()
        return final_state

    job_name = job_def.get("name", "Untitled")
    db_job = client.get_job(job_name)
    if db_job:
        client.update_job(db_job["job_id"], job_def)
        client.close()
    else:
        client.create_job(job_def)
        client.close()


if __name__ == "__main__":
    typer.run(deploy)
