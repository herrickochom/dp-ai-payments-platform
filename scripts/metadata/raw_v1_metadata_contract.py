
import ast, hashlib, importlib.util, json, re
from datetime import datetime, timezone
from pathlib import Path
import yaml
ROOT=Path(__file__).resolve().parents[2]
PATTERN=re.compile(r'raw/v1/(?P<mode>real-time|streaming|batch)/category=(?P<category>[^/]+)/source_group=(?P<group>[^/]+)/source_system=(?P<system>[^/]+)/year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/topic=(?P<topic>[^/]+)/partition=(?P<partition>0|[1-9][0-9]*)/offset=(?P<offset>0|[1-9][0-9]*)/record\.avro',re.ASCII)
class MetadataContract:
    def __init__(self, root=ROOT):
        root=Path(root)
        delivery=root/'platform/gcp/kafka_pubsub_bridge/source_delivery_v1.py'
        admission_bytes=delivery.read_bytes()
        if hashlib.sha256(admission_bytes).hexdigest()!='fd60213936ed098c81f88afcd60250e01f27a50cf66f9225970b4aa7f74e6973':
            raise ValueError('Authoritative admission policy checksum mismatch')
        tree=ast.parse(admission_bytes)
        values=[
            n.value for n in tree.body
            if isinstance(n,ast.Assign)
            and any(isinstance(t,ast.Name)
                    and t.id=='APPROVED_SOURCE_TOPICS'
                    for t in n.targets)
        ]
        if len(values)!=1:
            raise ValueError('Authoritative admission policy missing')
        node=values[0]
        if not isinstance(node,ast.Call) or not isinstance(node.func,ast.Name) or node.func.id!='frozenset' or len(node.args)!=1 or node.keywords:
            raise ValueError('Invalid admission policy')
        self.approved_source_topics=frozenset(ast.literal_eval(node.args[0]))
        if len(self.approved_source_topics)!=3:
            raise ValueError('Admission policy requires review')

        p=root/'contracts/metadata/raw-v1/raw-v1-metadata-contract.json'
        self.contract=json.loads(p.read_text())
        if hashlib.sha256(p.read_bytes()).hexdigest()!='4ab88237b1efb3e4240ab860ac48b87fc14199abcbe690a508adeb1f09b34556': raise ValueError('Unapproved metadata contract')
        paths={}
        for key,ref in self.contract['source_contracts'].items():
            p=root/ref['path']
            if hashlib.sha256(p.read_bytes()).hexdigest()!=ref['sha256']: raise ValueError('Source contract changed')
            paths[key]=p
        entries=yaml.safe_load(paths['topic_registry'].read_text())['topics']
        self.topics={e['name']:e for e in entries}
        self.routes=json.loads(paths['routing_contract'].read_text())['routes']
        if len(entries)!=38 or len(self.topics)!=38 or self.topics.keys()!=self.routes.keys(): raise ValueError('Topic registry mismatch')
        for topic,route in self.routes.items():
            if route['processing_mode']!=self.topics[topic]['processing_mode']: raise ValueError('Mode mismatch')
        spec=importlib.util.spec_from_file_location('approved_source_envelope',paths['source_envelope'])
        self.envelope=importlib.util.module_from_spec(spec); spec.loader.exec_module(self.envelope)
    def source_admitted(self,topic):
        route=self.routes.get(topic)
        return bool(route and (
            route['status']=='legacy_payment_event'
            or (
                route['status']=='contract_only'
                and topic in self.approved_source_topics
            )
        ))
    def classify_topic(self,topic):
        route=self.routes[topic]
        family=route.get('category')
        if topic.startswith('cdc.'): family='mdm_cdc'
        elif topic.startswith('payments.lifecycle.'): family='payment_lifecycle'
        elif topic.startswith('mdm.'): family='mdm'
        elif family=='operations': family='shared_operations'
        return self.contract['enterprise_domain_taxonomy'][family]
    def derive(self,name,envelope):
        m=PATTERN.fullmatch(name)
        if not m: raise ValueError('Invalid Raw path')
        p=m.groupdict(); topic=p['topic']; route=self.routes.get(topic)
        if not self.source_admitted(topic): raise ValueError('Source not admitted by this utility')
        e=self.envelope.validate_envelope(envelope)
        day=datetime.fromtimestamp(e['timestamp_ms']//1000,timezone.utc).strftime('%Y/%m/%d')
        if (p['topic'],int(p['partition']),int(p['offset']),p['year']+'/'+p['month']+'/'+p['day'])!=(e['topic'],e['partition'],e['offset'],day): raise ValueError('Source coordinates mismatch')
        if e['partition']>=self.topics[topic]['partitions']: raise ValueError('Partition outside registry')
        for a,b in [('mode','processing_mode'),('category','category'),('group','source_group'),('system','source_system')]:
            if p[a]!=route[b]: raise ValueError('Routing mismatch')
        result={'data-domain':self.classify_topic(topic),'data-layer':self.contract['storage_layer'],'storage-contract-version':self.contract['storage_contract_version'],'metadata-contract-version':self.contract['contract_version'],'processing-mode':route['processing_mode'],'category':route['category'],'source-group':route['source_group'],'source-system':route['source_system'],'source-topic':e['topic'],'source-partition':str(e['partition']),'source-offset':str(e['offset'])}
        if result.keys()!=self.contract['mandatory_custom_metadata'].keys(): raise ValueError('Metadata keys mismatch')
        return result

def plan_metadata_update(before, proposed):
    for key in ('content_type','content_encoding','content_disposition','content_language','custom_time'):
        if before[key]!=('application/avro' if key=='content_type' else None): raise ValueError('Native metadata mismatch')
    if not before['crc32c'] or not before['generation'] or not before['metageneration']: raise ValueError('Missing integrity evidence')
    existing=dict(before['metadata'] or {})
    if any(k in existing and existing[k]!=v for k,v in proposed.items()): raise ValueError('Metadata conflict')
    merged={**existing,**proposed}
    return merged, merged!=existing or before['cache_control']!='no-store'

def apply_metadata_update(blob,before,proposed,*,read_fresh,read_bytes):
    merged,changed=plan_metadata_update(before,proposed)
    if not changed: return 'NO_CHANGE_REQUIRED'
    original=read_bytes(before)
    blob.metadata=merged; blob.cache_control='no-store'
    blob.patch(if_generation_match=before['generation'],if_metageneration_match=before['metageneration'])
    after=read_fresh()
    keys=('generation','crc32c','md5_hash','size','content_type','content_encoding','content_disposition','content_language','custom_time')
    if any(before[k]!=after[k] for k in keys) or after['metadata']!=merged or after['cache_control']!='no-store' or int(after['metageneration'])!=int(before['metageneration'])+1 or read_bytes(after)!=original: raise RuntimeError('Post-update verification failed')
    return 'PASS'

if __name__ == "__main__":
    raise SystemExit("Live CLI disabled pending operational integration approval")
