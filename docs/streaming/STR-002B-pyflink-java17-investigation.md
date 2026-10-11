# STR-002B PyFlink Java 17 investigation

Java 11 is available and viable: the official registry publishes `flink:1.18.1-scala_2.12-java11` for amd64 and arm64, while the repository contains no streaming-route or cross-service requirement that Flink use Java 17. Under Java 17, the minimal correct fix is to open `java.base/java.util` to unnamed modules on the ephemeral `flink run` client JVM, because that JVM launches `PythonDriver`, constructs the job graph, and executes the failing reflective access before submission; the running image's launcher accepts this through client JVM options, including `env.java.opts.client` or `JVM_ARGS` (registry manifest inspection; `platform/docker/dockerfiles/Dockerfile.flink:22`; running-container `/opt/flink/bin/flink:52-55`).

## Question 1 — Java 11 availability and viability

### 1. Published image

- Verified with the read-only command `docker manifest inspect flink:1.18.1-scala_2.12-java11 --verbose`: the tag exists for Linux amd64 at digest `sha256:a7d8467acd9233b8acd73bb35d58c22518519c9e0de0bdcea6c811de6d8eb504` and Linux arm64/v8 at `sha256:6bf63adaa7d6fccf8b30af07c6a3dedac11eaaaf1471b2e007bdeedc6eb66495`.
- The manifest identifies the image version as `1.18.1-scala_2.12-java11`, its base as `eclipse-temurin:11-jre-jammy`, and its official source as `apache/flink-docker`. This is direct registry evidence, not an inference.
- The repository itself pins only the Java 17 variant for Flink (`platform/docker/dockerfiles/Dockerfile.flink:22`).

### 2. Flink-specific Java requirement in the repository

- Repository search across `platform/`, Compose files, Dockerfiles, and `docs/` found one explicit Flink Java version: `FROM flink:1.18.1-scala_2.12-java17` (`platform/docker/dockerfiles/Dockerfile.flink:22`).
- No `java11`, `-java11`, or separate statement requiring Java 17 for the streaming route was found in those paths. The present Java 17 selection is therefore a pin, not a documented architectural constraint.

### 3. Other service constraints

| Service/image | Repository pin | Constraint on Flink |
|---|---|---|
| Trino | Build argument `TRINO_BASE_IMAGE`; locally supplied as Trino 477 | None expressed; its JDK is encapsulated by its image (`platform/docker/dockerfiles/Dockerfile.trino-coordinator:1-10`). |
| Kafka broker | `apache/kafka:4.3.1` | None expressed (`platform/docker/dockerfiles/Dockerfile.kafka-broker:1-10`). |
| Kafka init | `confluentinc/cp-kafka:7.5.0` | None expressed (`platform/docker/dockerfiles/Dockerfile.kafka-init:1`). |
| Nessie | `ghcr.io/projectnessie/nessie:0.108.4` | None expressed (`platform/docker/dockerfiles/Dockerfile.nessie:1`). |

No shared host JVM or repository-wide Java-version setting couples these image-contained services to Flink.

### 4. ADR and planning requirement

- Searches of the ADR, phased plan, file plan, and risk register found no Java, JDK, or JVM requirement for the streaming route (`docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md`; `docs/streaming/phased-plan.md`; `docs/streaming/file-plan.md`; `docs/streaming/risk-register.md`).
- The plans require an explicitly pinned Flink image, not Java 17 specifically (`docs/streaming/phased-plan.md:107`; `docs/streaming/STR-002B-preflight.md:141-148`).

### 5. Existing Java 11 precedent

- No repository Dockerfile explicitly pins a Java 11 base or Java 11 variant. Java-based products are mostly pinned by product tag and encapsulate their own runtime; only the Flink Dockerfile exposes the Java version in its tag (`platform/docker/dockerfiles/Dockerfile.flink:22`; `platform/docker/dockerfiles/Dockerfile.kafka-broker:1`; `platform/docker/dockerfiles/Dockerfile.nessie:1`; `platform/docker/dockerfiles/Dockerfile.trino-coordinator:1-2`).
- Consequently, Java 11 is viable for Flink but is not made “consistent” by another explicit repository Java 11 choice.

**Confidence: High.** The image tag was verified directly against the registry, and the complete requested repository scopes were searched.

## Question 2 — Minimal fix under Java 17

### 1. Running JobManager Java and option variables

Command:

```sh
docker exec dp-ai-payments-platform-flink-jobmanager sh -c 'echo "JAVA_HOME=$JAVA_HOME"; java -version; echo "JVM_ARGS=$JVM_ARGS"; echo "FLINK_ENV_JAVA_OPTS=$FLINK_ENV_JAVA_OPTS"; echo "env.java.opts=$JVM_ARGS"'
```

Exact output:

```text
JAVA_HOME=/opt/java/openjdk
openjdk version "17.0.14" 2025-01-21
OpenJDK Runtime Environment Temurin-17.0.14+7 (build 17.0.14+7)
OpenJDK 64-Bit Server VM Temurin-17.0.14+7 (build 17.0.14+7, mixed mode, sharing)
JVM_ARGS=
FLINK_ENV_JAVA_OPTS=
env.java.opts=
```

This verifies Java 17 and the absence of supplied JVM-opening options in the inspected shell.

### 2. Running configuration

Command:

```sh
docker exec dp-ai-payments-platform-flink-jobmanager sh -c 'grep -n "add-opens\|env.java.opts" /opt/flink/conf/flink-conf.yaml || echo "no add-opens or env.java.opts entries"'
```

Exact output:

```text
no add-opens or env.java.opts entries
```

### 3. Launcher option sources

The container contains `config.sh`, `flink`, `flink-console.sh`, and the standard daemon launchers under `/opt/flink/bin/` (running-container command `ls /opt/flink/bin/ | head -30`). Relevant exact matches are:

```text
/opt/flink/bin/config.sh:180:KEY_ENV_JAVA_OPTS="env.java.opts.all"
/opt/flink/bin/config.sh:184:KEY_ENV_JAVA_OPTS_CLI="env.java.opts.client"
/opt/flink/bin/config.sh:319:if [ -z "${FLINK_ENV_JAVA_OPTS}" ]; then
/opt/flink/bin/config.sh:320:    FLINK_ENV_JAVA_OPTS=$(readFromConfig ${KEY_ENV_JAVA_OPTS} "" "${YAML_CONF}")
/opt/flink/bin/config.sh:348:if [ -z "${FLINK_ENV_JAVA_OPTS_CLI}" ]; then
/opt/flink/bin/config.sh:349:    FLINK_ENV_JAVA_OPTS_CLI=$(readFromConfig ${KEY_ENV_JAVA_OPTS_CLI} "${DEFAULT_ENV_JAVA_OPTS_CLI}" "${YAML_CONF}")
/opt/flink/bin/config.sh:383:if [ -z "${JVM_ARGS}" ]; then
/opt/flink/bin/config.sh:384:    JVM_ARGS=""
/opt/flink/bin/flink:52:FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_CLI}"
/opt/flink/bin/flink:55:exec "${JAVA_RUN}" $JVM_ARGS $FLINK_ENV_JAVA_OPTS ... org.apache.flink.client.cli.CliFrontend "$@"
```

Thus `flink run` reads `JVM_ARGS` directly, reads global `env.java.opts.all`, and adds `env.java.opts.client` through `FLINK_ENV_JAVA_OPTS_CLI`. `FLINK_ENV_JAVA_OPTS` can also override the global configuration-derived value. `flink-console.sh` is for service processes and is not the launcher used by the submitted `flink run` command.

### 4. JVM in which the failure occurs

- `flink run` starts `CliFrontend` in the CLI JVM (`/opt/flink/bin/flink:52-55`). `CliFrontend.run()` builds a `PackagedProgram` and calls `ClientUtils.executeProgram`; `ClientUtils` invokes the program interactively before remote submission ([Apache Flink 1.18 `CliFrontend.java`](https://github.com/apache/flink/blob/release-1.18/flink-clients/src/main/java/org/apache/flink/client/cli/CliFrontend.java), [Apache Flink 1.18 `ClientUtils.java`](https://github.com/apache/flink/blob/release-1.18/flink-clients/src/main/java/org/apache/flink/client/ClientUtils.java)).
- For `-py`, that packaged program is `PythonDriver`, whose documented purpose is to launch the Python application as a subprocess and connect it to the same JVM via Py4J; exceptions captured from that path are rethrown as `ProgramAbortException` ([Apache Flink 1.18 `PythonDriver.java`](https://github.com/apache/flink/blob/release-1.18/flink-python/src/main/java/org/apache/flink/client/python/PythonDriver.java)).
- `PythonConfigUtil.configPythonOperator()` finishes Python transformation preparation by calling the failing `registerPythonBroadcastTransformationTranslator()`, which reflectively opens the backing `m` field of the unmodifiable map ([Apache Flink 1.18 `PythonConfigUtil.java`](https://github.com/apache/flink/blob/release-1.18/flink-python/src/main/java/org/apache/flink/python/util/PythonConfigUtil.java)).
- Therefore the exception occurs while the client constructs the pipeline/job graph, before it is serialized and submitted to the JobManager. The option must affect the ephemeral `flink run` CLI JVM; changing only JobManager daemon options would not address this call path.

### 5. Mechanisms that reach the CLI JVM

| Candidate | Read by this image's `flink` CLI? | Evidence |
|---|---|---|
| `JVM_ARGS` | Yes | Passed directly to Java by `/opt/flink/bin/flink:55`; initialised in `config.sh:383-384`. |
| `env.java.opts.all` | Yes | Read into `FLINK_ENV_JAVA_OPTS` by `config.sh:180,319-327`. |
| `env.java.opts.client` | Yes, and most narrowly scoped | Read at `config.sh:184,348-351`, appended at `flink:52`, passed at `flink:55`. |
| `FLINK_ENV_JAVA_OPTS` | Yes | If supplied, it bypasses the global config read and is passed by `flink:55` (`config.sh:319-327`). |
| Wrapper script | Only if it supplies one of the above before invoking `flink` | Inference from the verified launcher path; no repository wrapper was found. |

For the observed exception, the minimal Java 17 capability is `--add-opens=java.base/java.util=ALL-UNNAMED`; broader flags are not demonstrated by this stack trace.

### 6. Bundled and official guidance

- A read-only search under `/opt/flink/conf`, `/opt/flink/README.txt`, and `/opt/flink/docs` produced no bundled `add-opens` or `env.java.opts` guidance beyond the live configuration already reported.
- The image's `config.sh` itself documents the supported global and client-specific option keys through `KEY_ENV_JAVA_OPTS="env.java.opts.all"` and `KEY_ENV_JAVA_OPTS_CLI="env.java.opts.client"` (running-container `/opt/flink/bin/config.sh:180-184`).
- No primary Flink 1.18 documentation located during this investigation prescribed a PyFlink-specific complete set of Java 17 `--add-opens` flags. The single required opening above is derived directly from the observed `InaccessibleObjectException` and the verified reflection target, not claimed as a comprehensive future-proof flag set.

**Confidence: High.** Runtime launcher scripts, live Java state, and the Flink 1.18 client/Python source establish both the failing JVM and the option paths; only the completeness of any broader Java 17 flag set remains uncertain.

## Recommended fix

Prefer the published Java 11 Flink 1.18.1 image for this Day-0 route: it removes the Java 17 module-access incompatibility without introducing a repository-wide inconsistency or a documented architecture exception. If Java 17 is retained for an external reason, supply the required `java.util` module opening specifically to the `flink run` client JVM through the client JVM-option facility in Flink configuration (or an equivalent CLI environment variable), rather than applying it only to the long-running JobManager process. This identifies the required location and mechanism without specifying an implementation edit.

**Confidence: Medium.** Java 11 availability and lack of repository constraints are verified, but end-to-end job execution on the Java 11 image was not tested and external production constraints are unknown.

## Known unknowns

- Whether an external deployment, security baseline, or support policy requires Java 17 despite the absence of such a repository requirement.
- Whether later PyFlink paths or added connectors would require additional Java 17 module openings beyond `java.util`.
- Whether the Java 11 variant passes the full repository image build and PyFlink smoke test; no image was pulled, built, or started.
- Whether a later Flink patch release removes this reflective-access requirement.
