"""GCS immutable storage adapter for KP-001W. No live consumer or automatic ack."""
from google.api_core.exceptions import PreconditionFailed


class GCSImmutableStore:
    """Create once using GCS generation precondition; compare bytes on duplicate."""
    def __init__(self, bucket):
        self.bucket = bucket

    def create_if_absent(self, name: str, data: bytes) -> bool:
        blob = self.bucket.blob(name)
        try:
            blob.upload_from_string(
                data,
                content_type='application/json',
                if_generation_match=0,
                checksum='auto',
                retry=None,
            )
            return True
        except PreconditionFailed:
            # The core worker MUST read and compare the existing object before ack.
            return False

    def read(self, name: str) -> bytes:
        # Read the current generation; immutable naming and conflict comparison
        # remain enforced by worker.process().
        return self.bucket.blob(name).download_as_bytes()
