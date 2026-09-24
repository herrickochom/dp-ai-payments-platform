"""Run-scoped Nessie isolation and optimistic, fail-closed publication."""
import json,os,re,urllib.error,urllib.parse,urllib.request
from services.shared.security.runtime_security import validate_nessie_security
from services.shared.security.secret_provider import SecretUnavailable, require_secret
class NessiePublicationError(RuntimeError): pass
class NessieNotFound(NessiePublicationError): pass
class NessieConflict(NessiePublicationError): pass
class NessieUnknownWriteState(NessiePublicationError): pass

def branch_for_run(run_id):
    if not re.fullmatch(r"(?:tr|be)_[a-f0-9]{32}",run_id): raise NessiePublicationError("invalid transform run identity")
    return "transform_"+run_id

def publication_token():
    """Resolve the publication bearer token; absence stays a local-mode failure."""
    try: return require_secret("NESSIE_PUBLICATION_TOKEN")
    except SecretUnavailable: return ""

class NessiePublisher:
    def __init__(self,endpoint=None,token=None,timeout_seconds=10):
        self.endpoint=(endpoint or os.getenv("NESSIE_ENDPOINT","")).rstrip("/")
        self.token=token if token is not None else publication_token()
        self.timeout=timeout_seconds
        if not self.endpoint: raise NessiePublicationError("Nessie endpoint unavailable")
        mode=os.getenv("NESSIE_AUTH_MODE","bearer")
        if mode not in {"bearer","development-insecure"}: raise NessiePublicationError("invalid Nessie authentication mode")
        if mode=="bearer" and not self.token: raise NessiePublicationError("Nessie publication token unavailable")
        try:
            validate_nessie_security(
                self.endpoint,
                auth_mode=mode,
                token=self.token,
            )
        except ValueError as exc:
            raise NessiePublicationError(str(exc)) from exc

    def _request(self,method,path,payload=None):
        body=None if payload is None else json.dumps(payload,sort_keys=True,separators=(",",":")).encode()
        headers={"Accept":"application/json"}
        if body is not None: headers["Content-Type"]="application/json"
        if self.token: headers["Authorization"]="Bearer "+self.token
        req=urllib.request.Request(self.endpoint+"/api/v2"+path,data=body,method=method,headers=headers)
        try:
            with urllib.request.urlopen(req,timeout=self.timeout) as r: raw=r.read()
        except urllib.error.HTTPError as e:
            if e.code==404: raise NessieNotFound("Nessie reference not found") from e
            if e.code in {409,412}: raise NessieConflict(f"Nessie conflict HTTP {e.code}") from e
            raise NessiePublicationError(f"Nessie HTTP {e.code}") from e
        except (urllib.error.URLError,TimeoutError) as e:
            if method in {"POST","PUT","DELETE"}:
                raise NessieUnknownWriteState("Nessie write outcome unknown; reconciliation required") from e
            raise NessiePublicationError("Nessie request failed") from e
        try: return json.loads(raw or b"{}")
        except json.JSONDecodeError as e: raise NessiePublicationError("invalid Nessie JSON response") from e

    def _ref(self,p):
        r=p.get("reference",p)
        if not isinstance(r,dict) or not r.get("name") or not r.get("hash"):
            raise NessiePublicationError("incomplete Nessie reference")
        return r

    def get_reference(self,name):
        return self._ref(self._request("GET","/trees/"+urllib.parse.quote(name,safe="")))

    def create_run_branch(self,run_id,base_ref,base_hash):
        name=branch_for_run(run_id)
        try: existing=self.get_reference(name)
        except NessieNotFound: existing=None
        if existing is not None:
            if existing["hash"]!=base_hash:
                raise NessieConflict("existing run branch does not match recorded base hash")
            return existing
        q=urllib.parse.urlencode({"name":name,"type":"BRANCH"})
        return self._ref(self._request("POST","/trees?"+q,{"type":"BRANCH","name":base_ref,"hash":base_hash}))

    def promote_run(self,run_id,target_ref,expected_target_hash,expected_source_hash=None):
        source=self.get_reference(branch_for_run(run_id))
        if expected_source_hash is not None and source["hash"]!=expected_source_hash:
            raise NessieConflict("run branch changed since publication approval")
        target=self.get_reference(target_ref)
        if target["hash"]!=expected_target_hash:
            raise NessieConflict("publication target moved from recorded base hash")
        ref=urllib.parse.quote(target_ref+"@"+expected_target_hash,safe="")
        out=self._request("POST",f"/trees/{ref}/history/merge",{"fromRefName":source["name"],"fromHash":source["hash"]})
        if out.get("wasApplied") is False: raise NessieConflict("Nessie merge was not applied")
        return out
