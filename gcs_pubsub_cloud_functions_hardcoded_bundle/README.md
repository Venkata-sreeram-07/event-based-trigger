# Cloud Functions (2nd gen): GCS Pub/Sub -> Cloud Composer

This package contains only:
- `main.py`
- `requirements.txt`

All project, bucket, DAG, and Composer URL settings are hardcoded in `main.py`.
No application environment variables are required.

## Hardcoded configuration

- GCP project: `project-23dfabac-4beb-4269-a9d`
- GCS bucket: `event-based-trigger`
- Source prefix: `source-files/`
- Pub/Sub topic: `event-based-trigger`
- Composer DAG: `gcs_sales_csv_to_bigquery`
- Composer region: `us-central1`
- Composer Airflow web-server base URL:
  `https://47fdbcedc46d4314bf36f685480fdbc6-dot-us-central1.composer.googleusercontent.com`

The `/home` suffix from the browser URL is intentionally removed; the REST API is called from the web-server base URL.

## Deploy

Run these commands from this folder. The function region is set to `us-central1` as a default; change it only if you want the function deployed elsewhere.

```bash
gcloud config set project project-23dfabac-4beb-4269-a9d

gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  eventarc.googleapis.com \
  pubsub.googleapis.com \
  run.googleapis.com \
  --project=project-23dfabac-4beb-4269-a9d

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

No `--set-env-vars` flag is needed.

## Airflow API compatibility

The function tries Airflow REST API v2 first (Airflow 3), then v1 (Airflow 2) if the first endpoint returns HTTP 404 or 405. This avoids needing an Airflow-version environment variable.

## Runtime IAM and connectivity

The function runtime service account must:
- Have `roles/composer.user` on the project.
- Be authorized by the Airflow web server's RBAC configuration to create DAG runs.
- Be able to reach the Composer Airflow web server. If Composer uses IP access control, private networking, or VPC Service Controls, configure the required connectivity/access.

The service account in the deploy command is:
`gcs-composer-trigger@project-23dfabac-4beb-4269-a9d.iam.gserviceaccount.com`

Ensure it exists and has the required permissions before deployment.

## Composer DAG requirement

The DAG must use `schedule=None` and process only the object passed in `dag_run.conf`:

```python
dag_run = context["dag_run"]
event_conf = dag_run.conf or {}

bucket = event_conf["bucket"]
object_name = event_conf["object"]
generation = event_conf["generation"]
```

Do not scan and process every file under `source-files/` for each event. Verify the object generation before processing, and make BigQuery writes/archive operations idempotent and generation-aware.

## Event behavior

- Only `OBJECT_FINALIZE` events for `.csv` objects under `source-files/` in the configured bucket trigger the DAG.
- A deterministic DAG run ID prevents duplicate DAG runs for the same bucket/object/generation.
- HTTP 409 (already-existing DAG run) is treated as successful duplicate delivery.
- Other API/network failures are raised so Cloud Functions retry can retry the event.
- The function's Pub/Sub trigger creates its own managed subscription. Your existing `event-based-trigger-sub` subscription can remain separate.
