"""
Integration test for the urls-scraper BigQuery switch: runs the real
BigQueryTableManager and DataStats.start_workflow against the BigQuery
emulator (GCS calls are stubbed out since they are unrelated to this
migration). Verifies the urls_scrapper_statistics row actually lands in
BigQuery with the expected values.

Requires the BigQuery emulator running:
    docker run -d -p 9050:9050 -p 9060:9060 ghcr.io/goccy/bigquery-emulator:latest --project=test-project

Then:
    uv run pytest tests/test_bigquery_integration.py -v
"""
import sys
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

import pytest
from google.auth.credentials import AnonymousCredentials
from google.api_core.client_options import ClientOptions
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.bigquery_utils import BigQueryTableManager  # noqa: E402
from utils.datastats_utils import DataStats  # noqa: E402
from utils.config_loader import Config  # noqa: E402

PROJECT_ID = "test-project"
DATASET_ID = "datastats_raw_urls_test"


@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(
        project=PROJECT_ID,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint="http://localhost:9050"),
    )


def test_start_workflow_writes_statistics_to_bigquery(bq_client, monkeypatch):
    config = Config(
        JOB_TO_SCRAP="data analyst",
        DATASTATS_BUCKET_URLS="fake-urls-bucket",
        DATASTATS_BUCKET_UTILS="fake-utils-bucket",
        URL_TO_SCRAP="https://example.com/JOB_TO_SCRAP",
        PROJECT_ID=PROJECT_ID,
        BQ_DATASET=DATASET_ID,
    )

    datastats = DataStats(
        script_execution_start_time=datetime.now(),
        scraped_jobs_list=["job1", "job2", "job3"],
        matched_jobs_list=["job1"],
        config=config,
    )

    # Patch the manager's bigquery.Client() call to use our emulator client,
    # and stub out GCS interactions (out of scope for this migration).
    with patch("utils.datastats_utils.BigQueryTableManager") as MockManagerClass, \
         patch("utils.datastats_utils.GoogleUtils") as MockGoogleUtils:

        real_manager = BigQueryTableManager(project_id=PROJECT_ID, dataset_id=DATASET_ID, client=bq_client)
        MockManagerClass.return_value = real_manager
        MockGoogleUtils.return_value.file_exists.return_value = False

        datastats.start_workflow()

    rows = list(bq_client.query(
        f"SELECT job_to_scrap, jobs_scraped, jobs_scraped_matched FROM `{PROJECT_ID}.{DATASET_ID}.urls_scrapper_statistics`"
    ).result())

    assert len(rows) == 1
    assert rows[0].job_to_scrap == "data analyst"
    assert rows[0].jobs_scraped == 3
    assert rows[0].jobs_scraped_matched == 1
