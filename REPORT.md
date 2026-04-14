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

## Amazon EMR Configuration

The Amazon EMR cluster used for the project was configured with release `emr-7.12.0` and a custom application bundle containing `Hadoop 3.4.1` and `Spark 3.5.6`. The cluster used a minimal two-node layout consisting of one primary node and one core node, both configured as `c3.2xlarge` instances, with no task nodes and manual sizing. Each node used a `gp3` EBS root volume configured with `30 GiB`, `3000` provisioned IOPS, and `125 MiB/s` throughput.

Cluster-specific logging was enabled to the project S3 log prefix under `project-harpy-eagle/logs`. No Spark step was attached during cluster creation; instead, the Spark analysis job was submitted afterward as a separate EMR step. IAM settings were configured so that EMR could create its own service role and EC2 instance profile with restricted read and write access to the required project S3 prefixes. Network placement and SSH access were configured through the AWS console using the project VPC, subnet, and selected key pair, with infrastructure identifiers omitted from this report.

## EMR Execution Verification

After the cluster was created, the Spark analysis was submitted as a separate EMR step using `spark-submit` in cluster deploy mode. Step verification was performed with `aws emr describe-step`, and the recorded step state was `COMPLETED`. Output verification was then performed against the project S3 results prefix. The verification confirmed that `drivers_summary.json` was generated successfully and that multiple per-driver JSON files were written under `per_driver_speed_data/`. Cluster IDs, step IDs, bucket names, and other environment-specific identifiers should be replaced with placeholders in submitted screenshots or command transcripts.
