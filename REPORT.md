# Project Report

> A COMP4442 SERVICE AND CLOUD COMPUTING Group Project
> Project "Harpy Eagle"
> 
> **Authors:**
> - Frank Xikun Yang
> - Jim Jinkun Yang
> - Tommy Kam-Ho Lau
> - Jimmy Meisong He

## Project Overview
...

## Production Architecture

The deployed system uses `Amazon S3`, `Amazon EMR`, `Amazon DynamoDB`, `Amazon EC2`, and `Cloudflare Tunnel`. `S3` stores the Spark script, raw dataset, and EMR logs. `EMR` runs the Spark analysis job. The Spark job writes the processed driver summary and event data directly into DynamoDB. `EC2` hosts the Flask application with Gunicorn, and the web application reads directly from DynamoDB at request time. Cloudflare Tunnel exposes the EC2 service publicly without requiring public inbound web ports on the instance.

## Amazon EMR Configuration

The Amazon EMR cluster used for the project was configured with release `emr-7.12.0` and a custom application bundle containing `Hadoop 3.4.1` and `Spark 3.5.6`. The cluster used a minimal two-node layout consisting of one primary node and one core node, both configured as `c3.2xlarge` instances, with no task nodes and manual sizing. Each node used a `gp3` EBS root volume configured with `30 GiB`, `3000` provisioned IOPS, and `125 MiB/s` throughput.

Cluster-specific logging was enabled to the project S3 log prefix under `project-harpy-eagle/logs`. No Spark step was attached during cluster creation; instead, the Spark analysis job was submitted afterward as a separate EMR step. IAM settings were configured so that EMR could create its own service role and EC2 instance profile with restricted access to the required S3 prefixes and DynamoDB tables. Network placement and SSH access were configured through the AWS console using the project VPC, subnet, and selected key pair, with infrastructure identifiers omitted from this report.

## EMR Execution Verification

After the cluster was created, the Spark analysis was submitted as a separate EMR step using `spark-submit` in cluster deploy mode. Step verification was performed with `aws emr describe-step`, and the recorded step state was `COMPLETED`. Output verification was then performed against DynamoDB rather than a local or S3-hosted JSON result set. The verification confirmed that the driver summary table contained per-driver summary items and that the events table contained multiple event rows for individual drivers. Cluster IDs, step IDs, bucket names, table names, and other environment-specific identifiers should be replaced with placeholders in submitted screenshots or command transcripts.
