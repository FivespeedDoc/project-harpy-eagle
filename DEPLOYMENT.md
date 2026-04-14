# Deployment Guide

This guide documents one production architecture only:

- `S3` stores the Spark script, raw dataset, generated JSON results, and EMR logs
- `EMR` runs the Spark analysis job
- `EC2` hosts the Flask application with `Gunicorn`
- `Cloudflare Tunnel` exposes the EC2 web service

This architecture cleanly separates batch analytics from the web tier:

1. raw files are uploaded to `S3`
2. `EMR` runs `spark/spark_analysis.py`
3. JSON results are written back to `S3`
4. the EC2 web server syncs the `results/` prefix locally
5. `Gunicorn` serves the dashboard from local JSON files

## Final Architecture

Use exactly this setup:

1. create one S3 bucket or one project prefix in an existing bucket
2. upload the Spark script and dataset to `S3`
3. create an EMR cluster with `Spark`
4. submit the Spark analysis as an EMR step
5. launch one Ubuntu EC2 instance for the website
6. sync the S3 `results/` prefix onto the EC2 instance
7. run the Flask app with `Gunicorn`
8. expose the EC2 service with `Cloudflare Tunnel`

The EC2 host does not run Spark in this architecture.

## Part 1: Prepare S3

### 1. Create the S3 layout

Create a bucket or a base prefix such as:

```text
s3://PROJECT_BUCKET/project-harpy-eagle/
├── code/
├── dataset/detail-records/
├── results/
└── logs/
```

Recommended naming:

- Spark script: `s3://PROJECT_BUCKET/project-harpy-eagle/code/spark_analysis.py`
- raw data: `s3://PROJECT_BUCKET/project-harpy-eagle/dataset/detail-records/`
- generated results: `s3://PROJECT_BUCKET/project-harpy-eagle/results/`
- EMR logs: `s3://PROJECT_BUCKET/project-harpy-eagle/logs/`

Current project bucket example:

- base prefix: `s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle/`

### 2. Prepare AWS permissions

Two AWS permission paths are required:

- the local machine or CI environment that uploads assets and submits EMR steps needs an authenticated AWS CLI session
- the EC2 web server needs read access to the S3 `results/` prefix

Recommended IAM configuration:

- an EC2 instance role with `s3:GetObject` and `s3:ListBucket` for the project bucket
- the default EMR roles, or custom EMR roles, with access to:
  - read `code/`
  - read `dataset/detail-records/`
  - write `results/`
  - write `logs/`

## Part 2: Upload the Spark Assets to S3

### 3. Upload the script and dataset

From the project root on the local machine:

```bash
aws login
export AWS_REGION=ap-southeast-1
./scripts/upload_emr_assets_to_s3.sh s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle
```

This uploads:

- [spark/spark_analysis.py](/Users/jimyang/PycharmProjects/project-harpy-eagle/spark/spark_analysis.py) to `code/`
- `dataset/detail-records/` to `dataset/detail-records/`

If the dataset lives outside the default local path, pass it explicitly:

```bash
aws login
export AWS_REGION=ap-southeast-1
./scripts/upload_emr_assets_to_s3.sh s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle /path/to/detail-records
```

Manual equivalent:

```bash
aws login
export AWS_REGION=ap-southeast-1
aws s3 cp spark/spark_analysis.py s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle/code/spark_analysis.py
aws s3 sync dataset/detail-records/ s3://project-harpy-eagle-641628981470-ap-southeast-1-an/project-harpy-eagle/dataset/detail-records/
```

## Part 3: Run Spark on EMR

### 4. Create the EMR cluster

Create an EMR cluster with these minimum characteristics:

- release line: current EMR 7.x release
- applications: `Spark`
- log URI: `s3://PROJECT_BUCKET/project-harpy-eagle/logs/`
- instance layout: at least one primary node and one core node

Notes:

- no `--master` argument should be passed to `spark/spark_analysis.py` on EMR
- the script already supports `s3://...` input and output paths

### 4A. Reference EMR Configuration Used in the Console

The EMR cluster used for this project was configured in the AWS web console with the following settings:

- cluster name: a project-specific cluster name
- Amazon EMR release: `emr-7.12.0`
- application bundle type: `Custom`
- installed applications:
  - `Hadoop 3.4.1`
  - `Spark 3.5.6`

Operating system configuration:

- Amazon Linux release provided by EMR
- automatic Amazon Linux updates enabled
- no custom AMI

Provisioning model:

- unified instance group mode
- manual sizing
- no EMR managed scaling policy
- no custom autoscaling configuration

Node layout:

- 1 primary node
- 1 core node
- 0 task nodes
- primary node high availability not enabled

Instance configuration:

- primary node instance type: `c3.2xlarge`
- core node instance type: `c3.2xlarge`
- each node uses a `gp3` EBS root volume
- root volume size: `30 GiB`
- provisioned IOPS: `3000`
- provisioned throughput: `125 MiB/s`

Step and logging configuration:

- no Spark step attached during cluster creation
- cluster-specific logging to the project S3 log prefix
- log prefix: `project-harpy-eagle/logs`

Security and IAM configuration:

- an EC2 key pair was selected for SSH access
- the cluster was launched inside a project VPC and subnet selected in the console
- EMR was allowed to create the service role
- EMR was allowed to create the EC2 instance profile
- S3 access was restricted to the required project prefixes rather than granting unrestricted access to all buckets

This reference configuration matches the project workflow:

1. upload Spark code and raw data to S3
2. create the EMR cluster
3. submit the Spark analysis as a separate EMR step
4. write generated JSON results back to S3
5. sync the S3 results onto the EC2 web server

### 5. Submit the Spark step

Use the helper script:

```bash
./deploy/emr/add_spark_step.sh j-XXXXXXXXXXXXX s3://PROJECT_BUCKET/project-harpy-eagle
```

This submits a Spark step equivalent to:

```bash
spark-submit --deploy-mode cluster s3://PROJECT_BUCKET/project-harpy-eagle/code/spark_analysis.py \
  --input s3://PROJECT_BUCKET/project-harpy-eagle/dataset/detail-records/ \
  --output s3://PROJECT_BUCKET/project-harpy-eagle/results/
```

### 6. Verify EMR output

After the step completes, confirm these objects exist in S3:

```text
s3://PROJECT_BUCKET/project-harpy-eagle/results/drivers_summary.json
s3://PROJECT_BUCKET/project-harpy-eagle/results/driver_behavior_records.json
s3://PROJECT_BUCKET/project-harpy-eagle/results/per_driver_speed_data/*.json
```

Example:

```bash
aws s3 ls s3://PROJECT_BUCKET/project-harpy-eagle/results/
aws s3 ls s3://PROJECT_BUCKET/project-harpy-eagle/results/per_driver_speed_data/ | head
```

### 6A. Verify Successful EMR Step Completion

Use the EMR step ID returned by `aws emr add-steps` and verify the step status:

```bash
aws emr describe-step \
  --cluster-id <EMR_CLUSTER_ID> \
  --step-id <EMR_STEP_ID> \
  --region ap-southeast-1
```

A successful run should show:

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
        "--output",
        "s3://PROJECT_BUCKET/project-harpy-eagle/results/",
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

After a completed step, the S3 verification should show:

- `drivers_summary.json` in the `results/` prefix
- multiple per-driver JSON files in `results/per_driver_speed_data/`

Example verification commands:

```bash
aws s3 ls s3://PROJECT_BUCKET/project-harpy-eagle/results/
aws s3 ls s3://PROJECT_BUCKET/project-harpy-eagle/results/per_driver_speed_data/ | head
```

Expected outcome:

- a `drivers_summary.json` object exists
- a `per_driver_speed_data/` prefix exists
- multiple `<driver_id>.json` files are present under that prefix

Note:

- the S3 console or `aws s3 ls` may show zero-byte folder marker objects for prefixes
- the actual success criteria are the generated JSON result files, not the marker objects

## Part 4: Launch and Prepare the EC2 Web Server

### 7. Launch the EC2 instance

Use Ubuntu `24.04`.

Recommended instance size:

- `t3.small` or `t3.medium`

Security group:

- allow `22/tcp` from the administrator IP address

Because this guide uses Cloudflare Tunnel, `80/tcp` and `443/tcp` do not need to be exposed publicly.

Attach the EC2 IAM role that can read the S3 `results/` prefix.

### 8. Connect to the instance

```bash
ssh -i /path/to/ec2-key.pem ubuntu@EC2_PUBLIC_IP
```

### 9. Install system packages

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip unzip curl
```

The EC2 web host does not need Java or PySpark in this architecture.

### 9A. Install AWS CLI v2 on Ubuntu

AWS CLI v2 is the recommended installation path on the EC2 host.

Check the machine architecture first:

```bash
uname -m
```

If the output is `x86_64`, install AWS CLI v2 with:

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version
```

If the output is `aarch64`, install the ARM build:

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version
```

After installation, verify that the EC2 IAM role is available to the instance:

```bash
aws sts get-caller-identity
```

If the EC2 instance role was attached recently, the credentials may take a short time to appear. A reboot is usually not required.

### 10. Copy the repository to the instance

```bash
cd /opt
sudo git clone https://github.com/FivespeedDoc/project-harpy-eagle.git
sudo chown -R ubuntu:www-data /opt/project-harpy-eagle
cd /opt/project-harpy-eagle
```

### 11. Create the Python environment

```bash
cd /opt/project-harpy-eagle
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

The production web server only needs [requirements.txt](/Users/jimyang/PycharmProjects/project-harpy-eagle/requirements.txt).

## Part 5: Sync Results from S3 onto EC2

### 12. Prepare the web app environment file

```bash
cd /opt/project-harpy-eagle
sudo cp .env.example /etc/project-harpy-eagle.env
sudo chown root:root /etc/project-harpy-eagle.env
sudo chmod 644 /etc/project-harpy-eagle.env
sudo nano /etc/project-harpy-eagle.env
```

Recommended contents:

```bash
APP_HOST=127.0.0.1
APP_PORT=5000
RESULTS_DIR=/opt/project-harpy-eagle/results
DEFAULT_SPEED_BATCH_SIZE=50
MAX_SPEED_BATCH_SIZE=500
GUNICORN_BIND=127.0.0.1:8000
GUNICORN_WORKERS=2
GUNICORN_THREADS=4
GUNICORN_TIMEOUT=120
```

### 13. Run the first S3 sync

```bash
cd /opt/project-harpy-eagle
./scripts/sync_results_from_s3.sh s3://PROJECT_BUCKET/project-harpy-eagle/results/
```

Equivalent AWS CLI command:

```bash
aws s3 sync s3://PROJECT_BUCKET/project-harpy-eagle/results/ /opt/project-harpy-eagle/results/ --delete
```

### 14. Verify the local results directory

```bash
ls /opt/project-harpy-eagle/results
ls /opt/project-harpy-eagle/results/per_driver_speed_data | head
```

## Part 6: Run and Validate the Website

### 15. Smoke-test the Flask app

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
- `/ready` returns `200` when the synced result files are present

Stop the dev server with `Ctrl+C`.

### 16. Smoke-test Gunicorn

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

### 17. Install the systemd web service

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

## Part 7: Optional Automatic S3 Result Refresh

### 18. Prepare the sync environment file

```bash
sudo cp deploy/systemd/project-harpy-eagle-sync.env.example /etc/project-harpy-eagle-sync.env
sudo chown root:root /etc/project-harpy-eagle-sync.env
sudo chmod 644 /etc/project-harpy-eagle-sync.env
sudo nano /etc/project-harpy-eagle-sync.env
```

Example contents:

```bash
S3_RESULTS_URI=s3://PROJECT_BUCKET/project-harpy-eagle/results/
LOCAL_RESULTS_DIR=/opt/project-harpy-eagle/results
AWS_REGION=ap-southeast-1
```

### 19. Install the sync service and timer

```bash
sudo cp deploy/systemd/project-harpy-eagle-sync-results.service /etc/systemd/system/
sudo cp deploy/systemd/project-harpy-eagle-sync-results.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable project-harpy-eagle-sync-results.timer
sudo systemctl start project-harpy-eagle-sync-results.timer
```

Manual test:

```bash
sudo systemctl start project-harpy-eagle-sync-results.service
sudo systemctl status project-harpy-eagle-sync-results.service
sudo systemctl status project-harpy-eagle-sync-results.timer
```

The timer refreshes local results every five minutes.

## Part 8: Expose the Site with Cloudflare Tunnel

### 20. Install `cloudflared`

```bash
sudo mkdir -p --mode=0755 /usr/share/keyrings
curl -fsSL https://pkg.cloudflare.com/cloudflare-public-v2.gpg | sudo tee /usr/share/keyrings/cloudflare-public-v2.gpg >/dev/null
echo "deb [signed-by=/usr/share/keyrings/cloudflare-public-v2.gpg] https://pkg.cloudflare.com/cloudflared any main" | sudo tee /etc/apt/sources.list.d/cloudflared.list
sudo apt-get update
sudo apt-get install -y cloudflared
```

### 21. Create the tunnel in the Cloudflare dashboard

In the Cloudflare Zero Trust web console:

1. open `Networks` -> `Tunnels`
2. create a new tunnel
3. choose the `Cloudflared` connector type
4. name it `project-harpy-eagle`
5. keep the setup page open so the generated tunnel token can be copied

### 22. Configure the public hostname

Still in the Cloudflare dashboard, add a public hostname for the tunnel:

- hostname: the chosen public hostname, for example `harpy.example.com`
- service type: `HTTP`
- URL: `http://localhost:8000`

### 23. Install the tunnel as a service

```bash
sudo cloudflared service install YOUR_TUNNEL_TOKEN
sudo systemctl enable cloudflared
sudo systemctl start cloudflared
sudo systemctl status cloudflared
```

### 24. Verify the public hostname

```bash
curl http://127.0.0.1:8000/health
```

Then open:

```text
https://PUBLIC_HOSTNAME
```

## Part 9: Refresh Workflow

When the dataset changes:

1. upload the new dataset files to S3
2. submit the EMR Spark step again
3. wait for the EMR step to finish
4. let the EC2 sync timer refresh the local results, or run the sync script manually

If the Spark script changes, upload [spark/spark_analysis.py](/Users/jimyang/PycharmProjects/project-harpy-eagle/spark/spark_analysis.py) to the S3 `code/` prefix again before submitting the next EMR step.

## Troubleshooting

### EMR step fails

- verify the EMR cluster includes the `Spark` application
- verify the EMR roles can read `code/` and `dataset/`
- verify the EMR roles can write `results/` and `logs/`
- verify the dataset exists under `s3://PROJECT_BUCKET/project-harpy-eagle/dataset/detail-records/`

### S3 results are missing on EC2

- verify the EC2 instance role can read the S3 bucket
- run [scripts/sync_results_from_s3.sh](/Users/jimyang/PycharmProjects/project-harpy-eagle/scripts/sync_results_from_s3.sh) manually
- inspect `sudo journalctl -u project-harpy-eagle-sync-results.service -n 100 --no-pager`

### `/ready` returns `503`

- verify `/opt/project-harpy-eagle/results/drivers_summary.json` exists
- verify `/opt/project-harpy-eagle/results/driver_behavior_records.json` exists
- verify `/opt/project-harpy-eagle/results/per_driver_speed_data/*.json` exists
- confirm `RESULTS_DIR=/opt/project-harpy-eagle/results`

### Cloudflare Tunnel does not connect

- verify the public hostname is attached to the correct tunnel in the Cloudflare dashboard
- verify the tunnel origin URL is `http://localhost:8000`
- verify `systemctl status cloudflared`
- inspect `sudo journalctl -u cloudflared -n 100 --no-pager`

## Suggested Report Evidence

For the report, capture screenshots of:

1. the S3 bucket layout
2. the EMR cluster details
3. the EMR step completion status
4. the S3 `results/` prefix
5. the EC2 instance details
6. `systemctl status project-harpy-eagle`
7. `systemctl status project-harpy-eagle-sync-results.timer`
8. the website homepage
9. the Cloudflare tunnel page in the web console or `systemctl status cloudflared`
