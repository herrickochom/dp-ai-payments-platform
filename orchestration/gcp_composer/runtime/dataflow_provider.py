"""Read-only Dataflow provider reconciliation. No job submission."""
from recovery_contracts import JobIdentity, classify_jobs


def _normalise_state(value):
    """Normalise GAPIC enum values without treating an unknown state as DONE."""
    name=getattr(value, 'name', None)
    if isinstance(name, str) and name.startswith('JOB_STATE_'):
        return name
    if isinstance(value, str) and value.startswith('JOB_STATE_'):
        return value
    return 'JOB_STATE_UNKNOWN'

class DataflowVisibilityError(RuntimeError): pass

class ReadOnlyDataflowProvider:
    def __init__(self, client, *, project, region):
        if not project or not region: raise ValueError('project and region required')
        self.client, self.project, self.region = client, project, region

    def find_by_name(self, project, region, name):
        if (project, region) != (self.project, self.region):
            raise DataflowVisibilityError('project or region not approved')
        try:
            request={'project_id':project,'location':region,'filter':'ALL'}
            response=self.client.list_jobs(request=request)
            raw=list(getattr(response,'jobs',[]) or [])
        except Exception:
            # An unavailable listing is UNKNOWN, not evidence of absence.
            return None
        matches=[]
        for j in raw:
            if getattr(j,'name',None) == name:
                labels=dict(getattr(j,'labels',{}) or {})
                matches.append({'id':str(getattr(j,'id','')),
                    'name':name,'project':project,'region':region,
                    'spec_digest':labels.get('pdm_spec_digest'),
                    'state':_normalise_state(getattr(j,'current_state',None))})
        return matches

    def list_matching(self, identity: JobIdentity):
        identity.validate()
        return self.find_by_name(identity.project,identity.region,identity.job_name)

    def get_job(self, identity: JobIdentity, job_id: str):
        identity.validate()
        if (identity.project,identity.region)!=(self.project,self.region):
            raise DataflowVisibilityError('scope mismatch')
        job=self.client.get_job(request={'project_id':identity.project,
            'location':identity.region,'job_id':job_id})
        labels=dict(getattr(job,'labels',{}) or {})
        return {'id':str(getattr(job,'id','')),'name':getattr(job,'name',None),
                'project':identity.project,'region':identity.region,
                'spec_digest':labels.get('pdm_spec_digest'),
                'state':_normalise_state(getattr(job,'current_state',None))}


def assess_visibility(provider, identity):
    """Do not submit on UNKNOWN or EMPTY; both require reconciliation."""
    jobs=provider.list_matching(identity)
    return classify_jobs(identity,jobs)
