"""Pure local simulation of Bronze batch sink sequencing; never connects to BigQuery.

This module is NOT a live BigQuery executor. It does not integrate the Stage 14E.2
SQL contract, production run ledger, Beam pipeline, or cloud sink.
"""
from dataclasses import dataclass, field

class ContractFailure(ValueError):
    pass

@dataclass
class FakeBronzeSink:
    bronze: set = field(default_factory=set)
    staging: dict = field(default_factory=dict)
    committed: dict = field(default_factory=dict)
    events: list = field(default_factory=list)
    fail_at: str | None = None

    def _step(self, name):
        self.events.append(name)
        if self.fail_at == name:
            self.fail_at = None
            raise RuntimeError('injected failure: ' + name)

    def execute(self, run_id, identities, quarantine_count=0, expected_input=None):
        if not isinstance(run_id, str) or not run_id or not run_id.replace('-', '').replace('_', '').isalnum():
            raise ContractFailure('invalid run identifier')
        if type(quarantine_count) is not int or quarantine_count < 0:
            raise ContractFailure('invalid quarantine count')
        identities = list(identities)
        for identity in identities:
            if (not isinstance(identity, tuple) or len(identity) != 3
                or not isinstance(identity[0], str) or not identity[0]
                or type(identity[1]) is not int or identity[1] < 0
                or type(identity[2]) is not int or identity[2] < 0):
                raise ContractFailure('invalid Kafka identity')
        if len(identities) != len(set(identities)):
            raise ContractFailure('duplicate staging identity')
        if expected_input is None:
            expected_input = len(identities) + quarantine_count
        if type(expected_input) is not int or expected_input != len(identities) + quarantine_count:
            raise ContractFailure('source accounting mismatch')
        snapshot = frozenset(identities)
        if run_id in self.committed:
            old = self.committed[run_id]
            if old['identities'] != snapshot or old['quarantined'] != quarantine_count:
                raise ContractFailure('run ID reused with different input')
            self._step('replay_verified')
            return dict(old, inserted=0, replay=True)
        if run_id in self.staging and self.staging[run_id] != snapshot:
            raise ContractFailure('run ID staging conflict')
        self._step('stage')
        self.staging[run_id] = snapshot
        self._step('verify_staging')
        if self.staging[run_id] != snapshot:
            raise ContractFailure('staging mismatch')
        # Simulate an atomic MERGE: do not mutate Bronze before the merge step passes.
        to_insert = snapshot - self.bronze
        self._step('merge')
        self.bronze.update(to_insert)
        self._step('verify_sink')
        if not snapshot.issubset(self.bronze):
            raise ContractFailure('sink reconciliation mismatch')
        result = {'run_id': run_id, 'identities': snapshot,
                  'source': expected_input, 'accepted': len(snapshot),
                  'quarantined': quarantine_count, 'inserted': len(to_insert),
                  'replay': False}
        self._step('commit')
        self.committed[run_id] = dict(result)
        return result
