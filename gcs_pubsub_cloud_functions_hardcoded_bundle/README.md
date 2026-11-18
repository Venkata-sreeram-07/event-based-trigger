# Event-Driven GCS → Pub/Sub → Cloud Functions (2nd gen) → Cloud Composer → BigQuery

A reusable reference implementation for starting an Apache Airflow DAG in Cloud Composer whenever a file is uploaded to a Google Cloud Storage (GCS) bucket.

The repository's Cloud Function source package contains only:

- `main.py`
- `requirements.txt`

The sample implementation hardcodes its project, bucket, source prefix, Pub/Sub topic, Composer DAG ID, region, and Composer Airflow web-server base URL in `main.py`. **No application environment variables are required.** For a new deployment, update the constants in `main.py` and the corresponding values in the commands below.

> This README separates reusable setup instructions from the example project's values. Replace every `<PLACEHOLDER>` before running a command. Commands using the sample project are provided as examples, not as values to copy into another project.

---

## 1. Architecture

```text
CSV/object uploaded to GCS
          |
          | OBJECT_FINALIZE notification
          v
     Pub/Sub topic
          |
          v
Cloud Functions (2nd gen)
  - receives Pub/Sub CloudEvent
  - decodes the GCS notification
  - validates bucket, prefix, event type and file extension
  - creates a deterministic Airflow DAG run ID
          |
          | Airflow REST API: POST DAG run
          v
Cloud Composer / Apache Airflow
  - receives bucket, object name and generation in dag_run.conf
  - processes only that object generation
          |
          v
       BigQuery
  - load / validate / MERGE idempotently
          |
          v
  GCS archive prefix
  - archive only after successful processing
```

### Example project configuration

| Component | Example value |
|---|---|
| GCP project ID | `project-23dfabac-4beb-4269-a9d` |
| Region | `us-central1` |
| GCS bucket | `event-based-trigger` |
| Input prefix | `source-files/` |
| Archive prefix | `archive/` |
| Pub/Sub topic | `event-based-trigger` |
| Optional inspection subscription | `event-based-trigger-sub` |
| Cloud Function | `gcs-file-to-composer` |
| Function generation/runtime | 2nd gen / Python 3.12 |
| Function entry point | `gcs_file_to_composer` |
| Composer DAG ID | `gcs_sales_csv_to_bigquery` |
| BigQuery dataset/table | `event_based_trigger.processed_sales_data` |
| Composer Airflow API base URL | `https://47fdbcedc46d4314bf36f685480fdbc6-dot-us-central1.composer.googleusercontent.com` |

The Composer URL is the Airflow web-server base URL. Remove `/home` if you copied the browser UI URL; do not include `/home` in the REST API base URL.

---

## 2. Prerequisites

You need:

1. A Google Cloud project with billing enabled.
2. The Google Cloud CLI (`gcloud`) installed and authenticated, or Cloud Shell.
3. Permission to enable APIs, create resources, and grant IAM roles. An administrator may need to perform the IAM steps.
4. A GCS bucket, a Pub/Sub topic, and a Cloud Composer environment (or permission to create them).
5. A Composer DAG deployed and visible in the Airflow UI.
6. The Cloud Function source files `main.py` and `requirements.txt`.

Authenticate and select the project:

```bash
gcloud auth login
gcloud config set project <PROJECT_ID>
gcloud config get-value project
```

For the example project:

```bash
gcloud config set project project-23dfabac-4beb-4269-a9d
```

Set shell variables for the commands in this guide. These are **Cloud Shell variables used to avoid repeating values**, not Cloud Function application environment variables:

```bash
export PROJECT_ID="<PROJECT_ID>"
export REGION="<REGION>"
export BUCKET="<GCS_BUCKET_NAME>"
export TOPIC="<PUBSUB_TOPIC_NAME>"
export SOURCE_PREFIX="source-files/"
export FUNCTION_NAME="gcs-file-to-composer"
export RUNTIME_SA="<FUNCTION_RUNTIME_SERVICE_ACCOUNT_EMAIL>"
```

Example values for the project used in this repository:

```bash
export PROJECT_ID="project-23dfabac-4beb-4269-a9d"
export REGION="us-central1"
export BUCKET="event-based-trigger"
export TOPIC="event-based-trigger"
export SOURCE_PREFIX="source-files/"
export FUNCTION_NAME="gcs-file-to-composer"
export RUNTIME_SA="gcs-composer-trigger@project-23dfabac-4beb-4269-a9d.iam.gserviceaccount.com"
```

---

## 3. Enable the required Google Cloud APIs

Enable the APIs used by the event trigger, function deployment, Composer, storage and BigQuery:

```bash
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  eventarc.googleapis.com \
  pubsub.googleapis.com \
  run.googleapis.com \
  storage.googleapis.com \
  composer.googleapis.com \
  bigquery.googleapis.com \
  --project="${PROJECT_ID}"
```

Some APIs may already be enabled, and some may be enabled automatically during resource creation. Enabling an API does not itself create the corresponding resource.

Check API status:

```bash
gcloud services list --enabled --project="${PROJECT_ID}"
```

---

## 4. Create or verify the GCS bucket

Check whether the bucket exists:

```bash
gcloud storage buckets describe "gs://${BUCKET}"
```

If it does not exist, create it. Choose a location appropriate for your workload and data-residency requirements:

```bash
gcloud storage buckets create "gs://${BUCKET}" \
  --project="${PROJECT_ID}" \
  --location="${REGION}" \
  --uniform-bucket-level-access
```

The bucket name must be globally unique. If the bucket already exists, do not run the create command again.

The prefixes `source-files/` and `archive/` are object-name prefixes, not folders that must be separately created. They appear in the Console after objects are uploaded under those names.

Upload test files only under the input prefix:

```bash
gcloud storage cp ./sample.csv "gs://${BUCKET}${SOURCE_PREFIX}sample.csv"
```

---

## 5. Create or verify the Pub/Sub topic

Check whether the topic exists:

```bash
gcloud pubsub topics describe "${TOPIC}" --project="${PROJECT_ID}"
```

If it does not exist, create it:

```bash
gcloud pubsub topics create "${TOPIC}" --project="${PROJECT_ID}"
```

List topics:

```bash
gcloud pubsub topics list --project="${PROJECT_ID}"
```

If your topic is in a different project from the bucket, use the full topic resource name when configuring the notification and grant the bucket project's Cloud Storage service agent publisher access on the topic in the topic-owning project. See [Google's cross-project notification guidance](https://cloud.google.com/storage/docs/reporting-changes).

---

## 6. Grant Cloud Storage permission to publish to Pub/Sub

Cloud Storage publishes bucket-change notifications using a Google-managed **Cloud Storage service agent**. This is not the same identity as your Cloud Function runtime service account.

### 6.1 Find the project number and service agent

```bash
gcloud projects describe "${PROJECT_ID}" \
  --format="value(projectNumber)"
```

You can also ask `gcloud` for the Cloud Storage service agent directly:

```bash
gcloud storage service-agent --project="${PROJECT_ID}"
```

The service agent normally has this format:

```text
service-<PROJECT_NUMBER>@gs-project-accounts.iam.gserviceaccount.com
```

For example, construct it in Cloud Shell as follows:

```bash
PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
GCS_SERVICE_AGENT="service-${PROJECT_NUMBER}@gs-project-accounts.iam.gserviceaccount.com"
echo "${GCS_SERVICE_AGENT}"
```

### 6.2 Grant Pub/Sub Publisher on the topic

Grant the service agent `roles/pubsub.publisher` on the destination topic:

```bash
gcloud pubsub topics add-iam-policy-binding "${TOPIC}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${GCS_SERVICE_AGENT}" \
  --role="roles/pubsub.publisher"
```

This topic-level binding is appropriately scoped to the destination topic. If you use a cross-project topic, run the binding command against the project that owns that topic, while keeping `GCS_SERVICE_AGENT` from the project that owns the bucket.

> **Note:** The `gcloud storage buckets notifications create` command also checks whether the Cloud Storage service agent can publish and attempts to grant the permission. The explicit IAM step above is useful when you want to configure and verify permissions separately or when the notification command reports a permission error. Allow a short time for IAM changes to propagate before creating the notification.

---

## 7. Connect the GCS bucket to the Pub/Sub topic

Cloud Storage bucket Pub/Sub notification configurations are managed with the `gcloud` CLI (or API/Terraform); this configuration is not created by deploying the Cloud Function.

### 7.1 Check for an existing notification first

```bash
gcloud storage buckets notifications list "gs://${BUCKET}"
```

If an appropriate notification already exists for the same topic, event type and prefix, do not create another one. Duplicate notification configurations can result in duplicate event messages.

### 7.2 Create the notification configuration

```bash
gcloud storage buckets notifications create "gs://${BUCKET}" \
  --topic="${TOPIC}" \
  --event-types=OBJECT_FINALIZE \
  --object-prefix="${SOURCE_PREFIX}" \
  --payload-format=json
```

For the example project, the fully expanded command is:

```bash
gcloud config set project project-23dfabac-4beb-4269-a9d

gcloud storage buckets notifications create \
  gs://event-based-trigger \
  --topic=event-based-trigger \
  --event-types=OBJECT_FINALIZE \
  --object-prefix=source-files/ \
  --payload-format=json
```

**Important payload-format clarification:** for `gcloud storage buckets notifications create`, the accepted CLI value is `json` (or `none`). The JSON notification payload corresponds to the Cloud Storage JSON API payload format `JSON_API_V1`. Do not pass `JSON_API_V1` as the CLI flag value.

This configuration means:

- `OBJECT_FINALIZE`: publish when an object is successfully created or an existing object is overwritten.
- `--object-prefix=source-files/`: publish only for object names beginning with this prefix.
- `--payload-format=json`: include the GCS object metadata in the notification payload.
- Objects uploaded under `archive/` do not match the `source-files/` prefix and therefore do not trigger this notification.

### 7.3 Verify the notification

```bash
gcloud storage buckets notifications list "gs://${BUCKET}"
```

Save the notification configuration ID shown in the output for operational reference. Cloud Storage notification delivery is at-least-once and is not guaranteed to be ordered; design downstream processing to tolerate duplicates and out-of-order events.

---

## 8. Configure the Cloud Function runtime service account

The Cloud Function runtime service account is separate from the Cloud Storage service agent described above. It is the identity used by the function code when calling the Composer Airflow REST API.

### 8.1 Create the service account if required

Check whether it exists:

```bash
gcloud iam service-accounts describe "${RUNTIME_SA}" \
  --project="${PROJECT_ID}"
```

If it does not exist, create one. Set `RUNTIME_SA_NAME` to the account name without the domain:

```bash
RUNTIME_SA_NAME="gcs-composer-trigger"

gcloud iam service-accounts create "${RUNTIME_SA_NAME}" \
  --project="${PROJECT_ID}" \
  --display-name="GCS to Composer trigger runtime"
```

Then set the email variable:

```bash
RUNTIME_SA="${RUNTIME_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
```

### 8.2 Grant Composer access

A common project-level role for invoking Composer environment operations is `roles/composer.user`:

```bash
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${RUNTIME_SA}" \
  --role="roles/composer.user"
```

The function must also be allowed by the Airflow web server's own authentication/RBAC configuration to create DAG runs. A Composer IAM role by itself might not satisfy Airflow authorization. Configure the appropriate Airflow access for the runtime identity according to your Composer version and environment configuration.

Also verify that the function can reach the Composer web server. Private IP, IP access control, VPC Service Controls, ingress restrictions, or other network policies may require additional configuration.

Do not grant Owner or Editor merely to bypass permission errors. Add only the permissions required by your deployment and DAG.

---

## 9. Configure the Composer environment and DAG

1. Open **Google Cloud Console → Composer → Environments**.
2. Select the environment in the intended region.
3. Open the Airflow UI and verify that the DAG ID configured in `main.py` exists.
4. Confirm the Composer Airflow web-server base URL and update the hardcoded URL in `main.py` if deploying to a different environment.
5. Do not append `/home` to the base URL used to construct REST API endpoints.

The DAG should be event-driven and should process only the object identified in the event payload.

Minimum DAG requirements:

- Set `schedule=None` (or the equivalent unscheduled setting for your Airflow version).
- Read `bucket`, `object`, and `generation` from `dag_run.conf`.
- Do not list and process every object under `source-files/` on each event.
- Read/verify the exact object generation from the event.
- Make BigQuery writes idempotent, for example by using a stable business key and `MERGE` logic.
- Archive the object only after successful processing, using generation preconditions where supported.

Example `dag_run.conf` passed by the function:

```json
{
  "bucket": "<BUCKET_NAME>",
  "object": "source-files/<FILE_NAME>.csv",
  "generation": "<GCS_OBJECT_GENERATION>",
  "event_type": "OBJECT_FINALIZE",
  "pubsub_message_id": "<PUBSUB_MESSAGE_ID>"
}
```

The generation and message ID above are placeholders. Use the actual values supplied by the event.

---

## 10. Deploy the Cloud Function (2nd gen)

### 10.1 Review the hardcoded settings in `main.py`

Before deploying to a new project, verify/update at least:

- GCP project ID.
- GCS bucket name.
- Input object prefix.
- Expected Pub/Sub topic/trigger configuration, if referenced in code.
- Composer DAG ID.
- Composer region, if used by the code.
- Composer Airflow web-server base URL.

The function's Pub/Sub trigger is configured at deployment time. Ensure the topic in the deployment command is the topic receiving the GCS notifications.

### 10.2 Enable APIs

The API enablement step is in [Section 3](#3-enable-the-required-google-cloud-apis). Ensure it has completed before deploying.

### 10.3 Deploy from the folder containing `main.py` and `requirements.txt`

```bash
gcloud functions deploy "${FUNCTION_NAME}" \
  --gen2 \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --runtime=python312 \
  --source=. \
  --entry-point=gcs_file_to_composer \
  --trigger-topic="${TOPIC}" \
  --service-account="${RUNTIME_SA}" \
  --retry
```

Example command with this repository's configuration:

```bash
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

No `--set-env-vars` flag is needed for this implementation. The application configuration is hardcoded in `main.py`.

### 10.4 Verify deployment

```bash
gcloud functions describe "${FUNCTION_NAME}" \
  --gen2 \
  --region="${REGION}" \
  --project="${PROJECT_ID}"
```

The Cloud Functions (2nd gen) Pub/Sub trigger uses a Google-managed subscription/trigger integration. A separately created subscription (for example, `event-based-trigger-sub`) can remain for inspection or testing; it receives its own copy of messages and is not the function's trigger subscription.

---

## 11. Event handling and retry behavior

The sample function is designed to:

- Accept Pub/Sub-triggered CloudEvents.
- Decode the Base64-encoded GCS JSON notification payload.
- Process only `OBJECT_FINALIZE` events for the configured bucket.
- Accept only objects under the configured source prefix and with the expected `.csv` suffix.
- Pass the bucket, object name, generation, event type and Pub/Sub message ID to the Composer DAG.
- Generate a deterministic DAG run ID from the bucket, object name and generation so duplicate deliveries do not create multiple runs for the same object generation.
- Treat Airflow HTTP 409 (DAG run already exists) as an already-submitted duplicate.
- Raise other API/network failures so the event can be retried.

The function attempts the Airflow REST API v2 endpoint first and falls back to v1 when the endpoint returns HTTP 404 or 405, supporting common Airflow 3 and Airflow 2 deployments respectively. Confirm the endpoint and authorization behavior for your Composer environment.

---

## 12. Test the complete pipeline

1. Confirm the GCS notification exists:

   ```bash
   gcloud storage buckets notifications list "gs://${BUCKET}"
   ```

2. Upload a CSV under the source prefix:

   ```bash
   gcloud storage cp ./sample.csv "gs://${BUCKET}${SOURCE_PREFIX}sample.csv"
   ```

3. Check Pub/Sub topic metrics or use a separate inspection subscription to confirm a message was published. Avoid pulling messages from a subscription used by another consumer unless you understand the acknowledgement behavior.
4. Open **Cloud Logging** and inspect the Cloud Function logs.
5. Open the Composer Airflow UI and confirm a DAG run was created with the expected `dag_run.conf`.
6. Inspect task logs and confirm the exact object generation was processed.
7. Verify the expected rows in BigQuery.
8. Confirm the source object was archived only after successful processing.
9. Upload the same test object again or simulate a retry to verify duplicate handling and idempotency.

---

## 13. Example BigQuery schema (sample sales CSV)

The example dataset uses the following 15-column schema. Replace it with your own source schema when adapting this pattern.

| Column | BigQuery type |
|---|---|
| `Transaction_ID` | `STRING` |
| `Date` | `DATE` |
| `Time` | `TIME` |
| `Store_ID` | `STRING` |
| `Store_Location` | `STRING` |
| `Product_ID` | `STRING` |
| `Product_Name` | `STRING` |
| `Category` | `STRING` |
| `Quantity` | `INT64` |
| `Unit_Price` | `NUMERIC` |
| `Discount_Percent` | `NUMERIC` |
| `Total_Amount` | `NUMERIC` |
| `Payment_Method` | `STRING` |
| `Customer_Type` | `STRING` |
| `Cashier_ID` | `STRING` |

Example target table:

```text
<PROJECT_ID>.<DATASET_ID>.<TABLE_ID>
```

For this sample project:

```text
project-23dfabac-4beb-4269-a9d.event_based_trigger.processed_sales_data
```

---

## 14. Troubleshooting

| Symptom | Checks |
|---|---|
| No Pub/Sub message | Confirm bucket notification exists, topic is correct, prefix matches, and Cloud Storage service agent has `roles/pubsub.publisher` on the topic. |
| Notification creation fails with permission error | Verify the caller can update the bucket and manage the topic IAM policy; verify the Cloud Storage service agent identity and publisher binding. |
| Function is not invoked | Check function deployment, Pub/Sub trigger/topic, Eventarc status, service-agent permissions and function logs. |
| Function receives but ignores event | Check event type, bucket name, source prefix, object extension and payload parsing. |
| Composer API returns 401/403 | Check runtime service account, Composer IAM, Airflow RBAC/authentication and web-server access restrictions. |
| Composer API returns 404/405 | Check the Composer web-server base URL and Airflow REST API version/path. Ensure `/home` is not appended to the base URL. |
| Duplicate DAG run / HTTP 409 | Expected for duplicate delivery when the deterministic run ID already exists; verify the existing DAG run's status. |
| BigQuery contains duplicate rows | Make the load/MERGE idempotent using a stable key and account for retries and repeated object events. |
| Same object name uploaded more than once | Compare GCS generation values; process the exact generation supplied in the event. |
| Archive upload triggers pipeline | Confirm notification uses `--object-prefix=source-files/` and archive objects are written under a different prefix. |

---

## 15. Production considerations

- **At-least-once delivery:** Cloud Storage notifications and Pub/Sub can deliver duplicate messages. Design for duplicates rather than assuming exactly-once processing.
- **Ordering:** Notifications are not guaranteed to arrive in object-update order. Use object generation and metageneration preconditions where relevant.
- **Idempotency:** Make DAG submission, BigQuery writes and archive operations safe to retry.
- **Generation-aware reads:** Object names can be overwritten. A name alone does not uniquely identify the version that generated an event.
- **Archive safety:** Archive only after downstream processing succeeds. Avoid deleting or moving a newer object version by using generation preconditions.
- **Failure handling:** Monitor Cloud Function retries, dead-letter/retry behavior where configured, Composer DAG failures and Cloud Logging alerts.
- **Schema validation:** Define behavior for malformed CSVs, empty files, unexpected headers, schema drift and invalid records.
- **Large files/concurrency:** Size Composer workers and task resources for expected file sizes and upload rates. Control concurrency to avoid overloading BigQuery or downstream systems.
- **Notification lifecycle:** Cloud Storage may remove a notification configuration if it cannot publish to the destination topic for an extended period. Monitor the configuration and topic permissions.
- **Security:** Prefer dedicated service accounts and least-privilege IAM. Do not commit credentials, service-account keys or secrets to source control.

---

## 16. Useful commands quick reference

```bash
# Select project
gcloud config set project <PROJECT_ID>

# Enable APIs
gcloud services enable cloudfunctions.googleapis.com cloudbuild.googleapis.com \
  eventarc.googleapis.com pubsub.googleapis.com run.googleapis.com \
  storage.googleapis.com composer.googleapis.com bigquery.googleapis.com \
  --project=<PROJECT_ID>

# Get project number and Cloud Storage service agent
gcloud projects describe <PROJECT_ID> --format="value(projectNumber)"
gcloud storage service-agent --project=<PROJECT_ID>

# Create topic if needed
gcloud pubsub topics create <TOPIC_NAME> --project=<PROJECT_ID>

# Grant Cloud Storage service agent publisher access
gcloud pubsub topics add-iam-policy-binding <TOPIC_NAME> \
  --project=<PROJECT_ID> \
  --member="serviceAccount:service-<PROJECT_NUMBER>@gs-project-accounts.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"

# Check GCS notification configurations
gcloud storage buckets notifications list gs://<BUCKET_NAME>

# Create GCS -> Pub/Sub notification
gcloud storage buckets notifications create gs://<BUCKET_NAME> \
  --topic=<TOPIC_NAME> \
  --event-types=OBJECT_FINALIZE \
  --object-prefix=source-files/ \
  --payload-format=json

# Deploy Cloud Function (2nd gen)
gcloud functions deploy <FUNCTION_NAME> --gen2 \
  --project=<PROJECT_ID> --region=<REGION> --runtime=python312 \
  --source=. --entry-point=gcs_file_to_composer \
  --trigger-topic=<TOPIC_NAME> \
  --service-account=<RUNTIME_SERVICE_ACCOUNT> --retry
```

---

## 17. Official documentation

- [Cloud Storage Pub/Sub notifications overview](https://cloud.google.com/storage/docs/pubsub-notifications)
- [Configure Pub/Sub notifications for Cloud Storage](https://cloud.google.com/storage/docs/reporting-changes)
- [Create a GCS notification configuration with gcloud](https://cloud.google.com/sdk/gcloud/reference/storage/buckets/notifications/create)
- [Get the Cloud Storage service agent](https://cloud.google.com/storage/docs/getting-service-agent)
- [Cloud Functions (2nd gen) deployment with gcloud](https://cloud.google.com/functions/docs/deploy)
- [Cloud Composer documentation](https://cloud.google.com/composer/docs)

