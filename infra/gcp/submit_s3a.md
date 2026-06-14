# Submitting Exp 5 s3a jobs to Dataproc Serverless

`s3://commoncrawl` is public Open Data — read it anonymously, no AWS creds. The
Serverless runtime (2.2 ≈ Hadoop 3.3.x) does not bundle the S3A connector, so pull it
via `spark.jars.packages` and configure the anonymous provider. Verify the Hadoop
version of your runtime and match `hadoop-aws` to it (a mismatch is the usual failure).

Pass via `DATAPROC_EXTRA_PROPERTIES` (the hook added to `submit_job.sh`) —
comma-separated, no spaces — and ship the Exp 5 package with `DATAPROC_EXTRA_PKGS`:

    DATAPROC_EXTRA_PKGS="experiments/__init__.py experiments/exp5_wat" \
    DATAPROC_EXTRA_PROPERTIES="spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem,spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider,spark.hadoop.fs.s3a.endpoint=s3.us-east-1.amazonaws.com" \
    DATAPROC_IMAGE=<tag> ./infra/gcp/submit_job.sh experiments/exp5_wat/project_cdx.py ...

Match `hadoop-aws` to the runtime's Hadoop version (Serverless 2.2 ≈ 3.3.x); a mismatch
is the usual failure. If anonymous reads fail or hit limits, fall back to env-based creds from `.env`
(`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) via
`spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider`
injected with `spark.executorEnv.*` / driver env — **never** put the secret in a logged
`--properties` value (the repo is public). Anonymous is the default and the safe path.

Note: `parse_wat.py` reads WAT via fsspec/`s3fs` (anonymous, `anon=True`) inside a
`flatMap`, independent of the Hadoop connector; the connector matters for the cc-index
`spark.read.parquet` in `project_cdx.py`. Both read `s3://commoncrawl` anonymously.
