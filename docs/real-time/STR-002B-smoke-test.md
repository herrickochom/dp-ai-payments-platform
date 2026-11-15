# STR-002B Smoke Test Evidence

- Status: Passed
- Date: 2026-10-11
- Ticket: STR-002B / B1 (streaming-route substrate infrastructure)
- Related documents: phased-plan §5 (STR-002B), file-plan §3 (STR-002B), risk-register

## 1. Purpose

Confirm that the Flink session cluster runs, consumes from Kafka, checkpoints
to MinIO using the dedicated `flinkchkpt` identity, exposes Prometheus metrics,
and recovers from a TaskManager restart without losing state.

## 2. Environment

- Flink base image: `flink:1.18.1-scala_2.12-java17`
- Java: OpenJDK 17.0.14 (Temurin)
- Kafka connector: `flink-sql-connector-kafka-3.1.0-1.18.jar`
- Prometheus reporter: `flink-metrics-prometheus-1.18.1.jar`
- MinIO identity: `flinkchkpt` scoped to `flink/checkpoints/*` and `flink/savepoints/*`
- Compose profile: `streaming`
- `JVM_ARGS`: `--add-opens=java.base/java.util=ALL-UNNAMED`
- Both services have `hostname:` set to their stable compose service name

## 3. Evidence

### 3.1 Services healthy

`docker compose ps flink-jobmanager flink-taskmanager` — both Up ... (healthy).

### 3.2 Runtime prerequisites

- `java -version`: OpenJDK 17.0.14
- `echo $JVM_ARGS`: `--add-opens=java.base/java.util=ALL-UNNAMED`
- `which python`: `/usr/local/bin/python` (Python 3.10.12)
- `ls /opt/flink/lib/`: `flink-sql-connector-kafka-3.1.0-1.18.jar` present

### 3.3 JobManager advertises stable hostname

Log line: `Remoting started; listening on addresses :[pekko.tcp://flink@flink-jobmanager:6123]`.

### 3.4 Job submission

    docker exec dp-ai-payments-platform-flink-jobmanager flink run -py /tmp/j1.py

Observed: `Job has been submitted with JobID 4565494965ea6b1d4d7af63d8a5a7d56`.

### 3.5 Job running

    flink list --running

Observed: `4565494965ea6b1d4d7af63d8a5a7d56 : j1-lifecycle-state-skeleton (RUNNING)`.

### 3.6 Checkpoints written to MinIO

    mc ls flinkchk/$OBJECT_STORE_BUCKET/flink/checkpoints/

Observed: `4565494965ea6b1d4d7af63d8a5a7d56/` with `shared/` and `taskowned/` subdirectories.

### 3.7 Recovery from TaskManager restart

    docker compose --profile streaming restart flink-taskmanager
    sleep 20
    flink list --running

Observed: same JobID `4565494965ea6b1d4d7af63d8a5a7d56` still `RUNNING`.

## 4. Deviations

None for the final run. Earlier iterations encountered and resolved:
1. `python: No such file or directory` — fixed by `/usr/local/bin/python` symlink.
2. `Cannot found the Java class KafkaSource.builder` — fixed by adding the Kafka connector JAR to `/opt/flink/lib/`.
3. `InaccessibleObjectException: java.base does not "opens java.util"` — fixed by `JVM_ARGS` with `--add-opens`.
4. `Unknown resolve order: child-firstblob.server.port` — fixed by ensuring `flink-conf.yaml` ends with a newline and `FLINK_PROPERTIES` starts with a blank line.
5. Job failed recovery with `NoResourceAvailableException` — fixed by `hostname:` on both Flink services.

## 5. Result

Passed. B1's smoke test is complete.

## 6. Notes for future operators

- **Memory pressure is real on a 7.5 GB host.** Stop Airflow, Superset, Metabase,
  and Trino before Flink builds or heavy jobs.
- **Never use `docker compose down` for a single-service change.** Use
  `restart <service>` for mounted config, or `build <service>` +
  `up -d --force-recreate <service>` for image changes. When both Flink
  services must stay in sync, recreate them together.
- **The failing JVM for PyFlink is the CLI, not the JobManager daemon.**
  `JVM_ARGS` reaches the CLI JVM; `env.java.opts.all` does not.
- **PyFlink requires connector JARs on the Flink classpath.** They are fetched
  from Maven Central in `Dockerfile.flink`.
- **Flink in Docker uses the container hostname as its advertised RPC identity.**
  Set `hostname:` to a stable name in compose; otherwise the JobManager
  advertises a container ID that changes on recreation.
- **`flink-conf.yaml` must end with a trailing newline.**
- **PyFlink invokes `python`, not `python3`.** The Dockerfile creates the symlink.

## 7. Artifacts

- `platform/docker/dockerfiles/Dockerfile.flink`
- `platform/flink/conf/flink-conf.yaml`
- `platform/flink/jobs/j1_lifecycle_state_skeleton.py`
- `platform/minio/policies/flink-checkpoint.json`
- `platform/minio/provision_object_store_identities.py`
- `docker-compose.yaml` (Flink services, `streaming` profile, `flink-data` volume)
- `docs/streaming/STR-002B-preflight.md`
- `docs/streaming/STR-002B-minio-iam-discovery.md`
- `docs/streaming/STR-002B-pyflink-java17-investigation.md`
