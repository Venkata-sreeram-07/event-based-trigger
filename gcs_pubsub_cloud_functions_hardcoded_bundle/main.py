import base64
import hashlib
import json
import logging
from datetime import datetime, timezone
from urllib.parse import quote

import functions_framework
import google.auth
from google.auth.transport.requests import AuthorizedSession

# -------------------------------------------------------------------
# Project configuration - intentionally hardcoded as requested.
# -------------------------------------------------------------------
PROJECT_ID = "project-23dfabac-4beb-4269-a9d"

EXPECTED_BUCKET = "event-based-trigger"
EXPECTED_PREFIX = "source-files/"
DAG_ID = "gcs_sales_csv_to_bigquery"

# Use the Composer Airflow web-server base URL (not the /home UI path).
AIRFLOW_WEBSERVER_URL = (
    "https://47fdbcedc46d4314bf36f685480fdbc6-dot-us-central1."
    "composer.googleusercontent.com"
).rstrip("/")

# Try Airflow 3 REST API first, then Airflow 2 if the endpoint is not found.
AIRFLOW_API_VERSIONS = ("2", "1")

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

# Reuse credentials/session across warm invocations.
CREDENTIALS, _ = google.auth.default(
    scopes=[AUTH_SCOPE],
    quota_project_id=PROJECT_ID,
)
AUTHORIZED_SESSION = AuthorizedSession(CREDENTIALS)


def decode_gcs_notification(cloud_event):
    """
    Decode the GCS Pub/Sub notification delivered as a Pub/Sub CloudEvent.
    GCS notification attributes contain eventType, bucketId, objectId and
    objectGeneration. message.data contains Base64-encoded object metadata.
    """
    event_data = cloud_event.data or {}
    message = event_data.get("message", {})
    attributes = message.get("attributes", {}) or {}

    encoded_data = message.get("data")
    if not encoded_data:
        raise ValueError("Pub/Sub message does not contain message.data")

    decoded = base64.b64decode(encoded_data).decode("utf-8")
    object_metadata = json.loads(decoded)

    event_type = attributes.get("eventType")
    bucket = attributes.get("bucketId") or object_metadata.get("bucket")
    object_name = attributes.get("objectId") or object_metadata.get("name")
    generation = attributes.get("objectGeneration") or object_metadata.get("generation")
    message_id = message.get("messageId") or cloud_event["id"]

    return {
        "event_type": event_type,
        "bucket": bucket,
        "object": object_name,
        "generation": str(generation) if generation is not None else None,
        "message_id": str(message_id),
    }


def make_dag_run_id(bucket: str, object_name: str, generation: str) -> str:
    """Stable run ID deduplicates Pub/Sub redelivery for the same object generation."""
    raw_key = f"{bucket}/{object_name}/{generation}"
    digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:40]
    return f"gcs_file_{digest}"


def trigger_composer_dag(conf: dict, dag_run_id: str) -> None:
    """Trigger Composer using the authenticated Airflow REST API."""
    encoded_dag_id = quote(DAG_ID, safe="")
    last_response = None

    for api_version in AIRFLOW_API_VERSIONS:
        endpoint = f"api/v{api_version}/dags/{encoded_dag_id}/dagRuns"
        url = f"{AIRFLOW_WEBSERVER_URL}/{endpoint}"

        payload = {
            "dag_run_id": dag_run_id,
            "conf": conf,
        }
        if api_version == "2":
            payload["logical_date"] = datetime.now(timezone.utc).isoformat()

        response = AUTHORIZED_SESSION.post(url, json=payload, timeout=60)
        last_response = response

        # Pub/Sub is at-least-once. A deterministic run ID makes duplicates safe.
        if response.status_code == 409:
            logging.info("DAG run already exists; treating as duplicate: %s", dag_run_id)
            return

        # Airflow 2/3 endpoint mismatch: try the other supported API version.
        if response.status_code in (404, 405):
            logging.info(
                "Airflow API v%s endpoint unavailable (HTTP %s); trying next version",
                api_version,
                response.status_code,
            )
            continue

        if response.status_code not in (200, 201):
            logging.error(
                "Airflow API request failed. api_version=%s status=%s body=%s",
                api_version,
                response.status_code,
                response.text[:3000],
            )
            response.raise_for_status()

        logging.info(
            "Triggered Composer DAG. dag_id=%s dag_run_id=%s api_version=%s response=%s",
            DAG_ID,
            dag_run_id,
            api_version,
            response.text[:1000],
        )
        return

    if last_response is not None:
        logging.error(
            "Neither Airflow API v2 nor v1 worked. Last status=%s body=%s",
            last_response.status_code,
            last_response.text[:3000],
        )
        last_response.raise_for_status()

    raise RuntimeError("Unable to call the Composer Airflow REST API")


@functions_framework.cloud_event
def gcs_file_to_composer(cloud_event):
    """
    Cloud Functions (2nd gen) Pub/Sub trigger.
    Triggered by GCS object notifications published to event-based-trigger.
    """
    event = decode_gcs_notification(cloud_event)

    event_type = event["event_type"]
    bucket = event["bucket"]
    object_name = event["object"]
    generation = event["generation"]

    logging.info(
        "Received GCS event: event_type=%s bucket=%s object=%s generation=%s message_id=%s",
        event_type,
        bucket,
        object_name,
        generation,
        event["message_id"],
    )

    if event_type != "OBJECT_FINALIZE":
        logging.info("Ignoring event type: %s", event_type)
        return

    if bucket != EXPECTED_BUCKET:
        logging.info("Ignoring unexpected bucket: %s", bucket)
        return

    if not object_name or not object_name.startswith(EXPECTED_PREFIX):
        logging.info("Ignoring object outside source prefix: %s", object_name)
        return

    if not object_name.lower().endswith(".csv"):
        logging.info("Ignoring non-CSV object: %s", object_name)
        return

    if not generation:
        raise ValueError("GCS notification is missing objectGeneration/generation")

    dag_run_id = make_dag_run_id(bucket, object_name, generation)
    dag_conf = {
        "bucket": bucket,
        "object": object_name,
        "generation": generation,
        "event_type": event_type,
        "pubsub_message_id": event["message_id"],
    }

    # Raise API/network failures so Cloud Functions retry can redeliver the event.
    trigger_composer_dag(dag_conf, dag_run_id)
