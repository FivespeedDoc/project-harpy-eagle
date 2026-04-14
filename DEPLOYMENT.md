# Deployment Guide

This guide documents one production architecture only:

- `S3` stores the Spark script, raw dataset, and EMR logs
- `EMR` runs the Spark analysis job
- `DynamoDB` stores the processed summary and event data
- `EC2` hosts the Flask application with `Gunicorn`
- `Cloudflare Tunnel` exposes the EC2 web service

The production path is:

1. upload the Spark script and dataset to `S3`
2. create the DynamoDB tables
3. run the Spark job on `EMR`
4. write the processed output into DynamoDB
5. start the Flask application on `EC2`
6. expose the application through Cloudflare Tunnel

The EC2 host does not run Spark and does not maintain a local `results/` directory in this setup.

## Part 1: AWS Resources

### 1. Prepare the S3 prefix

Create a project prefix such as:

```text
s3://PROJECT_BUCKET/project-harpy-eagle/
├── code/
├── dataset/detail-records/
└── logs/
```

Recommended paths:

- Spark script: `s3://PROJECT_BUCKET/project-harpy-eagle/code/spark_analysis.py`
- raw dataset: `s3://PROJECT_BUCKET/project-harpy-eagle/dataset/detail-records/`
- EMR logs: `s3://PROJECT_BUCKET/project-harpy-eagle/logs/`

Current project bucket example:

- `s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle/`

### 2. Prepare IAM permissions

Three permission paths are required:

1. the local machine or CI environment that uploads assets and submits EMR steps needs an authenticated AWS CLI session
2. the EMR cluster needs:
   - read access to the S3 `code/` and `dataset/detail-records/` prefixes
   - write access to the S3 `logs/` prefix
   - read and write access to the DynamoDB tables
3. the EC2 web server needs read access to the DynamoDB tables

Recommended IAM model:

- EMR uses the default EMR service role and instance profile, or equivalent custom roles
- the EC2 instance role includes DynamoDB read access and `CloudWatchLogs` permissions if log shipping is desired

Important:

- the Spark job in this repository calls DynamoDB directly through the AWS SDK inside the EMR cluster
- because of that, the required DynamoDB permissions must be granted to the EMR EC2 instance profile used by the cluster nodes
- the EMR service role is not the role that application code uses for these DynamoDB calls

If the cluster is created in the AWS web console, this is configured in the IAM roles section during cluster creation. The EC2 instance profile selected there must include the DynamoDB permissions listed below.

For the EC2 host, the minimum DynamoDB permissions are:

- `dynamodb:DescribeTable`
- `dynamodb:Scan` on the summary table
- `dynamodb:Query` on the events table and its date index

For EMR, the Spark job additionally needs:

- `dynamodb:BatchWriteItem`
- `dynamodb:PutItem`
- `dynamodb:DeleteItem`
- `dynamodb:Scan`
- `dynamodb:DescribeTable`

Recommended DynamoDB resource scope for the EMR EC2 instance profile:

- the summary table ARN
- the events table ARN
- the events index ARN, for example `arn:aws:dynamodb:REGION:ACCOUNT_ID:table/project-harpy-eagle-driver-events/index/event-date-index`

## Part 2: Local Preparation

### 3. Authenticate the AWS CLI

On the local machine:

```bash
aws login
export AWS_REGION=ap-southeast-1
```

### 4. Create the DynamoDB tables

Run the repository helper from the project root:

```bash
./scripts/create_dynamodb_tables.sh \
  project-harpy-eagle-driver-summary \
  project-harpy-eagle-driver-events
```

This creates:

1. summary table
   - partition key: `driverID`
2. events table
   - partition key: `driverID`
   - sort key: `eventKey`
   - global secondary index: `event-date-index`
     - partition key: `eventDate`
     - sort key: `eventTimeDriverKey`

The helper uses `PAY_PER_REQUEST` billing to avoid capacity planning for this project workload.

### 5. Upload the Spark script and dataset

From the project root:

```bash
./scripts/upload_emr_assets_to_s3.sh \
  s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle
```

If the dataset is stored elsewhere locally:

```bash
./scripts/upload_emr_assets_to_s3.sh \
  s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle \
  /path/to/detail-records
```

Manual equivalent:

```bash
aws s3 cp spark/spark_analysis.py s3://PROJECT_BUCKET/project-harpy-eagle/code/spark_analysis.py
aws s3 sync dataset/detail-records/ s3://PROJECT_BUCKET/project-harpy-eagle/dataset/detail-records/
```

## Part 3: Run Spark on EMR

### 6. Create the EMR cluster

Create an EMR cluster with these minimum characteristics:

- release line: current EMR 7.x
- application bundle: `Spark`
- log URI: `s3://PROJECT_BUCKET/project-harpy-eagle/logs/`
- node layout: at least one primary node and one core node

Choose an EMR-supported instance type that fits the current EC2 vCPU quota for the AWS account and Region. Amazon EC2 On-Demand quotas are enforced in vCPUs, so the total vCPU count across the requested EMR nodes must stay within the available quota.

Practical rule:

- if the account has only the default low vCPU quota, start with a small 2-vCPU instance type for both the primary and core nodes
- if larger instances are required, request an EC2 service quota increase before creating the cluster

In the IAM roles section of the EMR console:

- keep the EMR service role for cluster provisioning
- select or create an EMR EC2 instance profile that has:
  - S3 access to the project `code/`, `dataset/detail-records/`, and `logs/` prefixes
  - DynamoDB access to the summary table, events table, and events date index

Without the DynamoDB permissions on the EC2 instance profile, the submitted Spark step will fail when it tries to write the processed output.

The Spark job should be submitted after cluster creation as a separate EMR step.

### 6A. Console configuration used for this project

The EMR cluster used for this project was created in the AWS web console with the following settings:

- Amazon EMR release: `emr-7.12.0`
- installed applications:
  - `Hadoop 3.4.1`
  - `Spark 3.5.6`
- provisioning model: unified instance group
- scaling mode: manual
- node layout:
  - 1 primary node
  - 1 core node
  - 0 task nodes
- instance type:
- primary: `c3.2xlarge`
- core: `c3.2xlarge`
- root volume:
  - `gp3`
  - `30 GiB`
  - `3000` IOPS
  - `125 MiB/s`
- cluster-specific logging enabled to the project `logs/` prefix
- no Spark step attached during cluster creation
- EMR-created service role and instance profile
- project VPC, subnet, and SSH key selected in the console

Environment-specific identifiers should be replaced with placeholders in submitted documentation.

Note:

- this `c3.2xlarge` example reflects the console configuration that was captured for documentation
- it is not guaranteed to fit the default EC2 quota in a new AWS account
- if the cluster creation fails with a vCPU quota error, reduce the instance size or request a quota increase and retry

### 6B. Cluster lifetime behavior

If the cluster is configured to terminate after the last step, each Spark rerun requires a new cluster.

If repeated testing is expected, configure the cluster to remain alive while idle and attach an auto-termination policy, for example:

```bash
aws emr put-auto-termination-policy \
  --cluster-id <EMR_CLUSTER_ID> \
  --auto-termination-policy IdleTimeout=3600 \
  --region ap-southeast-1
```

With that setup:

- completed steps leave the cluster in `WAITING`
- additional Spark steps can be submitted during the idle window
- EMR terminates the cluster automatically after the idle timeout expires

### 7. Submit the Spark step

Use the helper script:

```bash
./deploy/emr/add_spark_step.sh \
  j-XXXXXXXXXXXXX \
  s3://PROJECT_BUCKET/project-harpy-eagle \
  project-harpy-eagle-driver-summary \
  project-harpy-eagle-driver-events
```

This submits a Spark step equivalent to:

```bash
spark-submit --deploy-mode cluster \
  s3://PROJECT_BUCKET/project-harpy-eagle/code/spark_analysis.py \
  --input s3://PROJECT_BUCKET/project-harpy-eagle/dataset/detail-records/ \
  --summary-table project-harpy-eagle-driver-summary \
  --events-table project-harpy-eagle-driver-events \
  --aws-region ap-southeast-1 \
  --log-level ERROR
```

### 8. Verify the EMR step

The step submission returns a step ID such as `s-XXXXXXXXXXXXX`.

Check the state:

```bash
aws emr describe-step \
  --cluster-id <EMR_CLUSTER_ID> \
  --step-id <EMR_STEP_ID> \
  --region ap-southeast-1
```

Successful execution means:

- `Name`: `project-harpy-eagle-spark-analysis`
- `ActionOnFailure`: `CONTINUE`
- `State`: `COMPLETED`

Minimal successful example:

```json
{
  "Step": {
    "Id": "<EMR_STEP_ID>",
    "Name": "project-harpy-eagle-spark-analysis",
    "Config": {
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode",
        "cluster",
        "s3://PROJECT_BUCKET/project-harpy-eagle/code/spark_analysis.py",
        "--input",
        "s3://PROJECT_BUCKET/project-harpy-eagle/dataset/detail-records/",
        "--summary-table",
        "project-harpy-eagle-driver-summary",
        "--events-table",
        "project-harpy-eagle-driver-events",
        "--aws-region",
        "ap-southeast-1",
        "--log-level",
        "ERROR"
      ]
    },
    "ActionOnFailure": "CONTINUE",
    "Status": {
      "State": "COMPLETED"
    }
  }
}
```

### 9. Verify the DynamoDB output

After the step completes, confirm the data landed in DynamoDB.

Summary table check:

```bash
aws dynamodb scan \
  --table-name project-harpy-eagle-driver-summary \
  --limit 5 \
  --region ap-southeast-1
```

Events table check:

```bash
aws dynamodb query \
  --table-name project-harpy-eagle-driver-events \
  --key-condition-expression 'driverID = :driver_id' \
  --expression-attribute-values '{":driver_id":{"S":"xiexiao1000001"}}' \
  --limit 5 \
  --region ap-southeast-1
```

Successful verification means:

- the summary table contains one item per driver
- the events table contains multiple event rows per driver
- the Flask app can later return `200` from `/ready`

## Part 4: Launch and Prepare the EC2 Web Server

### 10. Launch the EC2 instance

Use Ubuntu `24.04`.

Recommended instance size:

- `t3.small` or `t3.medium`

Security group:

- allow `22/tcp` from the administrator IP address

Because this guide uses Cloudflare Tunnel, public `80/tcp` and `443/tcp` are not required.

Attach the EC2 IAM role that can read the DynamoDB tables.

### 11. Connect to the instance

```bash
ssh -i /path/to/ec2-key.pem ubuntu@EC2_PUBLIC_IP
```

### 12. Install system packages

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip unzip curl
```

The EC2 web host does not need Java or PySpark.

### 12A. Install AWS CLI v2 on Ubuntu

AWS CLI v2 is the recommended installation path on the EC2 host for diagnostics and operational verification.

Check the machine architecture:

```bash
uname -m
```

If the output is `x86_64`:

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version
```

If the output is `aarch64`:

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version
```

Verify the EC2 IAM role:

```bash
aws sts get-caller-identity
```

If the IAM role was attached recently, credentials may take a short time to appear. A reboot is usually not required.

### 13. Copy the repository to the instance

```bash
cd /opt
sudo git clone https://github.com/FivespeedDoc/project-harpy-eagle.git
sudo chown -R ubuntu:www-data /opt/project-harpy-eagle
cd /opt/project-harpy-eagle
```

### 14. Create the Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

The production web server only needs [requirements.txt](/Users/jimyang/PycharmProjects/project-harpy-eagle/requirements.txt).

### 15. Prepare the web app environment file

```bash
sudo cp .env.example /etc/project-harpy-eagle.env
sudo chown root:root /etc/project-harpy-eagle.env
sudo chmod 644 /etc/project-harpy-eagle.env
sudo nano /etc/project-harpy-eagle.env
```

Recommended contents:

```bash
AWS_REGION=ap-southeast-1
DDB_SUMMARY_TABLE=project-harpy-eagle-driver-summary
DDB_EVENTS_TABLE=project-harpy-eagle-driver-events
DDB_EVENTS_DATE_INDEX=event-date-index
APP_HOST=127.0.0.1
APP_PORT=5000
DEFAULT_SPEED_BATCH_SIZE=50
MAX_SPEED_BATCH_SIZE=500
GUNICORN_BIND=127.0.0.1:8000
GUNICORN_WORKERS=2
GUNICORN_THREADS=4
GUNICORN_TIMEOUT=120
```

## Part 5: Run and Validate the Website

### 16. Smoke-test the Flask app

```bash
cd /opt/project-harpy-eagle
source .venv/bin/activate
set -a
source /etc/project-harpy-eagle.env
set +a
python app.py
```

In another shell:

```bash
curl http://127.0.0.1:5000/health
curl http://127.0.0.1:5000/ready
```

Expected:

- `/health` returns `200`
- `/ready` returns `200` when the DynamoDB tables are accessible and populated

Stop the development server with `Ctrl+C`.

### 17. Smoke-test Gunicorn

```bash
cd /opt/project-harpy-eagle
source .venv/bin/activate
set -a
source /etc/project-harpy-eagle.env
set +a
gunicorn --config deploy/gunicorn.conf.py wsgi:app
```

In another shell:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/ready
curl http://127.0.0.1:8000/
```

Expected:

- `/health` returns `200`
- `/ready` returns `200`
- `/` serves the dashboard

Stop Gunicorn with `Ctrl+C`.

### 18. Install the systemd web service

```bash
sudo cp deploy/systemd/project-harpy-eagle.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable project-harpy-eagle
sudo systemctl start project-harpy-eagle
```

If the Linux username is not `ubuntu`, edit:

[project-harpy-eagle.service](/Users/jimyang/PycharmProjects/project-harpy-eagle/deploy/systemd/project-harpy-eagle.service)

Check status:

```bash
sudo systemctl status project-harpy-eagle
sudo journalctl -u project-harpy-eagle -n 100 --no-pager
```

## Part 6: Expose the Site with Cloudflare Tunnel

### 19. Install `cloudflared`

```bash
sudo mkdir -p --mode=0755 /usr/share/keyrings
curl -fsSL https://pkg.cloudflare.com/cloudflare-public-v2.gpg | sudo tee /usr/share/keyrings/cloudflare-public-v2.gpg >/dev/null
echo "deb [signed-by=/usr/share/keyrings/cloudflare-public-v2.gpg] https://pkg.cloudflare.com/cloudflared any main" | sudo tee /etc/apt/sources.list.d/cloudflared.list
sudo apt-get update
sudo apt-get install -y cloudflared
```

### 20. Create the tunnel in the Cloudflare dashboard

In the Cloudflare Zero Trust web console:

1. open `Networks` -> `Tunnels`
2. create a new tunnel
3. choose the `Cloudflared` connector type
4. name it `project-harpy-eagle`
5. keep the setup page open so the generated tunnel token can be copied

### 21. Configure the public hostname

In the Cloudflare dashboard, add a public hostname:

- hostname: the chosen public hostname, for example `harpy.example.com`
- service type: `HTTP`
- URL: `http://localhost:8000`

### 22. Install the tunnel as a service

```bash
sudo cloudflared service install YOUR_TUNNEL_TOKEN
sudo systemctl enable cloudflared
sudo systemctl start cloudflared
sudo systemctl status cloudflared
```

### 23. Verify the public hostname

```bash
curl http://127.0.0.1:8000/health
```

Then open:

```text
https://PUBLIC_HOSTNAME
```

## Part 7: Refresh Workflow

When the dataset changes:

1. upload the new dataset files to S3
2. submit the EMR Spark step again
3. wait for the EMR step to complete
4. the web app will read the updated DynamoDB rows directly

If the Spark script changes, upload [spark/spark_analysis.py](/Users/jimyang/PycharmProjects/project-harpy-eagle/spark/spark_analysis.py) to the S3 `code/` prefix again before submitting the next EMR step.

If the EMR cluster is configured to terminate after the last step, a new cluster must be created before the next rerun.

## Troubleshooting

### EMR step fails

- verify the EMR cluster includes the `Spark` application
- verify the EMR roles can read `code/` and `dataset/`
- verify the EMR roles can write `logs/`
- verify the EMR roles can read and write the DynamoDB tables
- verify the dataset exists under `s3://PROJECT_BUCKET/project-harpy-eagle/dataset/detail-records/`

### DynamoDB tables are empty

- verify `create_dynamodb_tables.sh` was run before the EMR step
- verify the Spark step completed with `State: COMPLETED`
- scan the summary table manually
- query the events table manually

### `/ready` returns `503`

- verify the EC2 instance role can read the DynamoDB tables
- verify `DDB_SUMMARY_TABLE`, `DDB_EVENTS_TABLE`, and `DDB_EVENTS_DATE_INDEX` match the actual table and index names
- verify the tables contain data
- inspect `sudo journalctl -u project-harpy-eagle -n 100 --no-pager`

### Cloudflare Tunnel does not connect

- verify the public hostname is attached to the correct tunnel in the Cloudflare dashboard
- verify the tunnel origin URL is `http://localhost:8000`
- verify `systemctl status cloudflared`
- inspect `sudo journalctl -u cloudflared -n 100 --no-pager`

## Suggested Report Evidence

For the report, capture screenshots of:

1. the S3 bucket layout
2. the DynamoDB table definitions
3. the EMR cluster details
4. the EMR step completion status
5. example DynamoDB summary and events queries
6. the EC2 instance details
7. `systemctl status project-harpy-eagle`
8. the website homepage
9. the Cloudflare tunnel page in the web console or `systemctl status cloudflared`
