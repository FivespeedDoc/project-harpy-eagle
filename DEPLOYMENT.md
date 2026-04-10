# Deployment Guide

This guide documents one deployment option only:

- one `EC2` instance
- `PySpark` running on that same instance
- `Flask + Gunicorn` running on that same instance
- `Cloudflare Tunnel` exposing the service, managed from the Cloudflare web console

This is the lowest-cost path that still satisfies the project requirements:

- the system is deployed on `AWS`
- the analysis is performed with `Spark`

## Final Architecture

Use exactly this setup:

1. launch one Ubuntu 24.04 EC2 instance
2. install Python, Java 17, and `cloudflared`
3. upload the repo to the instance
4. upload the dataset with SFTP into `dataset/detail-records/`
5. install the Python dependencies
6. run the Spark analysis locally with:
   ```bash
   python spark/spark_analysis.py --master local[*]
   ```
7. verify `results/` was generated
8. run the website with `Gunicorn`
9. expose the local Gunicorn service through `Cloudflare Tunnel`

No second AWS instance is required in this guide.

## Part 1: Launch and Prepare the EC2 Instance

### 1. Launch the instance

Use Ubuntu `24.04`.

Recommended instance size:

- `t3.medium`

If your instance is too small during the Spark step, resize that same instance temporarily and then resize it back later.

Security group:

- allow `22/tcp` from your IP

Because this guide uses Cloudflare Tunnel, you do not need to expose `80/tcp` or `443/tcp` publicly.

### 2. Connect to the instance

```bash
ssh -i /path/to/your-key.pem ubuntu@YOUR_EC2_PUBLIC_IP
```

### 3. Install system packages

Install:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip openjdk-17-jre-headless unzip curl
```

Verify Java:

```bash
java -version
```

## Part 2: Upload the Project and Dataset

### 4. Copy the repository to the instance

Clone from GitHub:

```bash
cd /opt
sudo git clone https://github.com/FivespeedDoc/project-harpy-eagle.git
sudo chown -R ubuntu:www-data /opt/project-harpy-eagle
cd /opt/project-harpy-eagle
```

### 5. Create the Python environment

```bash
cd /opt/project-harpy-eagle
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-spark.txt
```

Notes:

- `requirements.txt` is for the website runtime
- `requirements-spark.txt` is for the Spark analysis step

### 6. Upload the dataset

Your Spark script expects:

```text
dataset/detail-records/
```

Since you said you will SFTP the dataset up separately, place the raw files under:

```text
/opt/project-harpy-eagle/dataset/detail-records/
```

Example target file:

```text
/opt/project-harpy-eagle/dataset/detail-records/detail_record_2017_01_02_08_00_00
```

## Part 3: Run Spark on the EC2 Instance

### 7. Run the Spark analysis

From the project root:

```bash
cd /opt/project-harpy-eagle
source .venv/bin/activate
python spark/spark_analysis.py --master local[*]
```

This should generate:

```text
results/drivers_summary.json
results/per_driver_speed_data/*.json
```

### 8. Verify the Spark output

```bash
ls /opt/project-harpy-eagle/results
ls /opt/project-harpy-eagle/results/per_driver_speed_data | head
```

Optional quick check:

```bash
python - <<'PY'
import json
from pathlib import Path
summary = json.loads(Path('/opt/project-harpy-eagle/results/drivers_summary.json').read_text())
print('drivers:', len(summary))
print('keys:', sorted(summary[0].keys()))
PY
```

## Part 4: Configure and Verify the Website

### 9. Prepare the environment file

Copy the env template:

```bash
cd /opt/project-harpy-eagle
sudo cp .env.example /etc/project-harpy-eagle.env
sudo chown root:root /etc/project-harpy-eagle.env
sudo chmod 644 /etc/project-harpy-eagle.env
```

Edit it:

```bash
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

The key setting is:

- `RESULTS_DIR=/opt/project-harpy-eagle/results`

### 10. Smoke-test the Flask app

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
- `/ready` returns `200` if the result files are present

Stop the dev server with `Ctrl+C`.

### 11. Smoke-test Gunicorn

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

## Part 5: Run the Website as a Service

### 12. Install the systemd service

Copy the unit file:

```bash
sudo cp deploy/systemd/project-harpy-eagle.service /etc/systemd/system/
```

If your Linux username is not `ubuntu`, edit:

[project-harpy-eagle.service](/Users/jimyang/PycharmProjects/project-harpy-eagle/deploy/systemd/project-harpy-eagle.service)

Then enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable project-harpy-eagle
sudo systemctl start project-harpy-eagle
```

Check logs:

```bash
sudo systemctl status project-harpy-eagle
sudo journalctl -u project-harpy-eagle -n 100 --no-pager
```

Now test locally:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/
```

## Part 6: Expose the Site with Cloudflare Tunnel

### 14. Prerequisites

Before starting:

- add your domain to Cloudflare
- move your domain nameservers to Cloudflare
- decide on the hostname you want to use, for example `harpy.example.com`

### 15. Install `cloudflared`

Cloudflare’s current Debian/Ubuntu instructions use their apt repository.

```bash
sudo mkdir -p --mode=0755 /usr/share/keyrings
curl -fsSL https://pkg.cloudflare.com/cloudflare-public-v2.gpg | sudo tee /usr/share/keyrings/cloudflare-public-v2.gpg >/dev/null
echo "deb [signed-by=/usr/share/keyrings/cloudflare-public-v2.gpg] https://pkg.cloudflare.com/cloudflared any main" | sudo tee /etc/apt/sources.list.d/cloudflared.list
sudo apt-get update
sudo apt-get install -y cloudflared
```

### 16. Create the tunnel in the Cloudflare dashboard

In the Cloudflare Zero Trust web console:

1. open `Networks` -> `Tunnels`
2. create a new tunnel
3. choose the `Cloudflared` connector type
4. name it `project-harpy-eagle`
5. keep the setup page open so you can copy the generated tunnel token

This guide intentionally uses the web console flow only. No `cloudflared` config file is needed in the repository.

### 17. Configure the public hostname in the web console

Still in the Cloudflare dashboard, add a public hostname for the tunnel:

- hostname: your chosen public name, for example `harpy.example.com`
- service type: `HTTP`
- URL: `http://localhost:8000`

The local origin must stay `http://localhost:8000` because Gunicorn is the web process listening on loopback.

### 18. Install the tunnel as a service

On the EC2 instance, run the install command from the Cloudflare dashboard and paste the tunnel token:

```bash
sudo cloudflared service install YOUR_TUNNEL_TOKEN
sudo systemctl enable cloudflared
sudo systemctl start cloudflared
sudo systemctl status cloudflared
```

If the service was already started during install, `systemctl start` is harmless.

### 19. Verify the public hostname

After the service starts:

```bash
curl http://127.0.0.1:8000/health
```

Then open:

```text
https://your-public-hostname.example.com
```

If the tunnel is configured correctly, Cloudflare should proxy the request to local Gunicorn and the website should load.

## Part 7: Refreshing the Results Later

Whenever the dataset changes:

1. upload new raw files to `dataset/detail-records/`
2. rerun:
   ```bash
   python spark/spark_analysis.py --master local[*]
   ```
3. the website will read the updated JSON files from `results/`

Because the website reads the JSON files on demand, you usually do not need to restart the web service after refreshing `results/`.

## Troubleshooting

### Spark fails on EC2

- verify Java 17 is installed
- verify `python -m pip show pyspark` works inside `.venv`
- verify the raw files exist under `dataset/detail-records/`
- if memory is too low, resize the same EC2 instance temporarily

### The website shows setup notices

- verify `RESULTS_DIR` points to `/opt/project-harpy-eagle/results`
- verify `drivers_summary.json` exists
- verify `per_driver_speed_data/*.json` exists
- call `curl http://127.0.0.1:8000/ready`

### Cloudflare Tunnel does not connect

- verify the hostname is in a zone managed by Cloudflare
- verify the public hostname is attached to the correct tunnel in the Cloudflare dashboard
- verify the dashboard origin URL is `http://localhost:8000`
- verify the token used with `cloudflared service install` belongs to that tunnel
- verify `systemctl status cloudflared`
- inspect `sudo journalctl -u cloudflared -n 100 --no-pager`

## Suggested Report Evidence

For the report, capture screenshots of:

1. the EC2 instance details
2. the Spark analysis command running on EC2
3. the generated `results/` directory
4. the website homepage
5. the summary panel
6. the speed monitor panel
7. `/health`
8. `systemctl status project-harpy-eagle`
9. the Cloudflare tunnel page in the web console or `systemctl status cloudflared`
