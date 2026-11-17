# Event-Driven GCS to BigQuery Pipeline

An event-driven data ingestion pipeline on Google Cloud Platform (GCP).
When a CSV file is uploaded to a Cloud Storage landing prefix, a Pub/Sub
notification triggers a second-generation Python Cloud Function. The
function validates the event and starts the relevant Cloud Composer
Airflow DAG through the Airflow REST API. The DAG processes the exact
uploaded object and loads/merges the data into BigQuery before archiving
the source file.

## Architecture

``` text
CSV upload
   |
   v
Cloud Storage bucket: event-based-trigger
   |  OBJECT_FINALIZE notification
   v
Pub/Sub topic: event-based-trigger
   |
   v
Cloud Functions (2nd gen, Python 3.12)
   |  validates bucket / prefix / file type
   |  sends bucket + object + generation
   v
Cloud Composer / Airflow REST API
   |
   v
DAG: gcs_sales_csv_to_bigquery
   |
   +----> BigQuery: event_based_trigger.processed_sales_data
   |
   +----> GCS archive/ (after successful processing)
```

## GCP resource configuration

  -------------------------------------------------------------------------------------------------------------------------------
  Component                           Value
  ----------------------------------- -------------------------------------------------------------------------------------------
  Project ID                          `project-23dfabac-4beb-4269-a9d`

  Region                              `us-central1`

  GCS bucket                          `event-based-trigger`

  Input prefix                        `source-files/`

  Archive prefix                      `archive/`

  Pub/Sub topic                       `event-based-trigger`

  Existing inspection subscription    `event-based-trigger-sub`

  Cloud Function                      `gcs-file-to-composer`

  Function generation/runtime         2nd gen / Python 3.12

  Function entry point                `gcs_file_to_composer`

  Composer DAG                        `gcs_sales_csv_to_bigquery`

  BigQuery dataset                    `event_based_trigger`

  BigQuery table                      `processed_sales_data`

  Composer Airflow base URL           `https://47fdbcedc46d4314bf36f685480fdbc6-dot-us-central1.composer.googleusercontent.com`
  -------------------------------------------------------------------------------------------------------------------------------

> The Composer URL above is the API base URL. Do not append `/home` when
> constructing the Airflow REST API endpoint.

## Repository layout

``` text
.
├── README.md
├── main.py
└── requirements.txt
```

## How the pipeline works

1.  A CSV file is uploaded under
    `gs://event-based-trigger/source-files/`.
2.  Cloud Storage emits an `OBJECT_FINALIZE` notification to the Pub/Sub
    topic.
3.  The Cloud Function receives the Pub/Sub CloudEvent and decodes the
    Base64-encoded JSON payload.
4.  The function validates:
    -   Event type is `OBJECT_FINALIZE`.
    -   Bucket is `event-based-trigger`.
    -   Object name starts with `source-files/`.
    -   Object name ends with `.csv`.
5.  The function creates a deterministic Airflow DAG run ID using the
    bucket, object name and object generation.
6.  It calls the Composer Airflow REST API and passes the exact object
    details in `dag_run.conf`.
7.  The Composer DAG processes that object generation, loads/merges
    records into BigQuery and archives the file after successful
    processing.

## GCP Console implementation

### 1. Select the GCP project

1.  Open the [Google Cloud Console](https://console.cloud.google.com/).
2.  Select project `project-23dfabac-4beb-4269-a9d`.
3.  Confirm you have permissions to create resources and configure IAM.

### 2. Enable required APIs

In **APIs & Services → Library**, search for and enable the APIs
required by the deployment:

-   Cloud Functions API
-   Cloud Build API
-   Eventarc API
-   Pub/Sub API
-   Cloud Run API
-   Cloud Composer API (if not already enabled)
-   Cloud Storage API
-   BigQuery API

### 3. Verify the Cloud Storage bucket

1.  Navigate to **Cloud Storage → Buckets**.
2.  Open `event-based-trigger`.
3.  Under **Objects**, verify the `source-files/` and `archive/`
    prefixes.
4.  Upload CSV files only to `source-files/`.

Example object:

``` text
gs://event-based-trigger/source-files/supermarket_sales_2024_11_13.csv
```

### 4. Verify the Pub/Sub topic and subscription

1.  Navigate to **Pub/Sub → Topics**.
2.  Verify topic `event-based-trigger`.
3.  Navigate to **Pub/Sub → Subscriptions**.
4.  Verify the existing inspection subscription
    `event-based-trigger-sub` is attached to the topic.

The Cloud Function Pub/Sub trigger creates/manages its own subscription.
The existing inspection subscription can remain in place; it receives
its own copy of published messages.

### 5. Configure the Cloud Storage notification

Cloud Storage Pub/Sub notification configurations are managed using
`gcloud` rather than the Cloud Storage Console UI.

Open **Cloud Shell** and run:

``` bash
gcloud config set project project-23dfabac-4beb-4269-a9d

gcloud storage buckets notifications create \
  gs://event-based-trigger \
  --topic=event-based-trigger \
  --event-types=OBJECT_FINALIZE \
  --payload-format=JSON_API_V1
```

To inspect the notification configuration:

``` bash
gcloud storage buckets notifications list \
  gs://event-based-trigger
```

If the notification is already configured and publishing messages, do
not create a duplicate notification.

### 6. Verify the Composer environment and DAG

1.  Navigate to **Composer → Environments**.
2.  Open the Composer environment in `us-central1`.
3.  Open the Airflow web interface.
4.  Verify DAG `gcs_sales_csv_to_bigquery` exists.

The DAG must be event-driven:

-   Set `schedule=None`.
-   Read `bucket`, `object` and `generation` from `dag_run.conf`.
-   Process only the exact object version supplied by the event.
-   Do not scan and process every object in `source-files/` for each
    event.
-   Make BigQuery writes and archive operations idempotent.

Example event configuration received by the DAG:

``` json
{
  "bucket": "event-based-trigger",
  "object": "source-files/supermarket_sales_2024_11_13.csv",
  "generation": "OBJECT_GENERATION",
  "event_type": "OBJECT_FINALIZE",
  "pubsub_message_id": "PUBSUB_MESSAGE_ID"
}
```

Replace the illustrative generation and message ID with the actual
values passed by the function.

### 7. Configure the runtime service account

Runtime service account:

``` text
gcs-composer-trigger@project-23dfabac-4beb-4269-a9d.iam.gserviceaccount.com
```

In **IAM & Admin → Service Accounts**, verify that the account exists.
If it does not, create it.

Grant the minimum permissions required. The service account needs
Composer access (commonly `roles/composer.user`) and must also be
authorized by Airflow to create DAG runs. Project IAM alone may not
satisfy Airflow's own authorization checks.

Also verify:

-   Cloud Storage's service agent can publish to the Pub/Sub topic.
-   Eventarc/Pub/Sub service agents have the required service-agent
    permissions.
-   Composer web-server access controls and network restrictions allow
    the function to reach the Airflow API.

Do not grant broad Owner or Editor permissions as a shortcut.

### 8. Deploy the Cloud Function through the Console

1.  Open **Cloud Run → Functions** (or search for Cloud Functions in the
    Console).
2.  Click **Create function** and choose **2nd gen**.
3.  Set:
    -   Function name: `gcs-file-to-composer`
    -   Region: `us-central1`
    -   Runtime: `Python 3.12`
    -   Entry point: `gcs_file_to_composer`
    -   Trigger: Cloud Pub/Sub
    -   Topic: `event-based-trigger`
4.  Select runtime service account
    `gcs-composer-trigger@project-23dfabac-4beb-4269-a9d.iam.gserviceaccount.com`.
5.  Add `main.py` and `requirements.txt` from this repository.
6.  Enable retries if the deployment flow exposes the option.
7.  Deploy and wait until the function is ready.

**Application environment variables are not required.** The project,
bucket, source prefix, DAG ID and Composer URL are hardcoded in
`main.py`, as requested.

## Optional deployment using Cloud Shell

If deploying from a local source directory or Cloud Shell instead of the
Console:

``` bash
gcloud config set project project-23dfabac-4beb-4269-a9d

gcloud functions deploy gcs-file-to-composer \
  --gen2 \
  --project=project-23dfabac-4beb-4269-a9d \
  --region=us-central1 \
  --runtime=python312 \
  --source=. \
  --entry-point=gcs_file_to_composer \
  --trigger-topic=event-based-trigger \
  --service-account=gcs-composer-trigger@project-23dfabac-4beb-4269-a9d.iam.gserviceaccount.com \
  --retry
```

## Test the end-to-end pipeline

1.  Open **Cloud Storage → Buckets → event-based-trigger →
    source-files/**.
2.  Upload a valid CSV file.
3.  Open **Pub/Sub** and verify a message is published.
4.  Open the function's **Logs** view and verify the event is received
    and validated.
5.  Open the Composer Airflow UI and verify a DAG run is created.
6.  Inspect task logs and confirm the BigQuery load/MERGE succeeds.
7.  Verify the resulting rows in BigQuery.
8.  Verify the source object is archived only after successful
    processing.

## BigQuery target schema

The expected source CSV contains these 15 fields:

  Column               BigQuery type
  -------------------- ---------------
  `Transaction_ID`     STRING
  `Date`               DATE
  `Time`               TIME
  `Store_ID`           STRING
  `Store_Location`     STRING
  `Product_ID`         STRING
  `Product_Name`       STRING
  `Category`           STRING
  `Quantity`           INT64
  `Unit_Price`         NUMERIC
  `Discount_Percent`   NUMERIC
  `Total_Amount`       NUMERIC
  `Payment_Method`     STRING
  `Customer_Type`      STRING
  `Cashier_ID`         STRING

Target table:

``` text
project-23dfabac-4beb-4269-a9d.event_based_trigger.processed_sales_data
```

## Reliability and operational considerations

-   **At-least-once delivery:** GCS notifications and Pub/Sub can
    deliver duplicates. Do not assume exactly-once delivery or strict
    ordering.
-   **Idempotent DAG runs:** The function derives a deterministic run ID
    from bucket, object and generation. A duplicate run response
    (HTTP 409) is treated as already submitted.
-   **Generation-aware processing:** An object name can be overwritten.
    Use the generation from the event to identify and read the correct
    object version.
-   **Idempotent BigQuery writes:** Use a stable business key and
    `MERGE` logic so retries do not duplicate records.
-   **Safe archiving:** Archive only after successful BigQuery
    processing. Use generation preconditions when copying/deleting
    objects.
-   **Retries and failures:** Allow transient failures to retry, and
    monitor repeated failures in Cloud Logging and Airflow.
-   **Schema validation:** Define behavior for malformed CSVs, empty
    files, unexpected columns and schema changes.
-   **Large files:** Consider task timeouts, memory, chunking and
    appropriate Composer worker sizing.

## Troubleshooting

  -----------------------------------------------------------------------
  Issue                               What to check
  ----------------------------------- -----------------------------------
  No Pub/Sub message                  GCS notification configuration,
                                      topic name, GCS service-agent
                                      Pub/Sub Publisher permission

  Function not invoked                Pub/Sub trigger, Eventarc trigger
                                      status, service-agent roles,
                                      function logs

  Event ignored                       `eventType`, bucket, object prefix
                                      and `.csv` suffix

  Composer 401/403                    Runtime service account, Composer
                                      IAM, Airflow authorization and
                                      web-server access

  Composer 404/405                    Composer Airflow base URL and REST
                                      API version

  DAG run fails                       `dag_run.conf`, object generation,
                                      GCS/BigQuery permissions and
                                      Airflow task logs

  Duplicate target rows               BigQuery `MERGE` key and
                                      idempotency logic

  File not archived                   BigQuery task status and
                                      generation-aware archive task

  Deployment fails                    Enabled APIs, build permissions,
                                      Eventarc service agent and
                                      deployment logs
  -----------------------------------------------------------------------

## Dependencies

`requirements.txt`:

``` text
functions-framework==3.*
google-auth>=2.29.0,<3.0.0
```

## Cloud Function source (`main.py`)

The complete deployed function source is included below.

``` python
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
```

## Useful Google Cloud documentation

-   [Cloud Storage Pub/Sub
    notifications](https://cloud.google.com/storage/docs/pubsub-notifications)
-   [Cloud Storage notification
    configuration](https://cloud.google.com/storage/docs/reporting-changes)
-   [Pub/Sub triggers for
    functions](https://cloud.google.com/run/docs/triggering/pubsub-triggers)
-   [Cloud Composer Airflow REST
    API](https://cloud.google.com/composer/docs/composer-3/access-airflow-api)
-   [BigQuery datasets](https://cloud.google.com/bigquery/docs/datasets)
-   [BigQuery tables](https://cloud.google.com/bigquery/docs/tables)

------------------------------------------------------------------------

**Project:** `project-23dfabac-4beb-4269-a9d`\
**Pipeline:** GCS → Pub/Sub → Cloud Functions → Cloud Composer →
BigQuery

Namah Sankaraaya 🙏
