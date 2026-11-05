# GCP-KP-001D: Guarded Kafka-to-Pub/Sub POC

Offline-first bridge for a **six-field source Avro record matching the isolated POC Pub/Sub schema exactly**. Existing technical Kafka events are NOT automatically mapped. This does not modify the payment producer, Kafka topics, Terraform, or cloud resources.

- Requires explicit `--live` and bounded `--max-messages` (1–10).
- Uses Schema Registry Avro deserialisation, strict field/type validation, and Pub/Sub JSON encoding.
- Kafka source topic, partition, offset and governed source system are Pub/Sub attributes.
- Pub/Sub publish future must succeed before synchronous Kafka commit.
- Delivery is at-least-once; duplicates remain possible if publishing succeeds and commit fails.
- Failure stops processing; no silent skip, commit, or DLQ claim. No production recovery yet.
- **Do not run live** until the actual source topic schema, IAM, and test-event mapping have been reviewed and approved.

Offline verification: `python3 -m unittest discover -s tests/gcp -p 'test_kp001d_bridge_offline.py' -v`
