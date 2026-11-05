"""Read-only billing export query; billing exports are delayed, never a hard cap.
A current observation cannot be inferred from a delayed billing export.
"""
from __future__ import annotations
from decimal import Decimal
import re
class BillingExportUnavailable(RuntimeError):pass

def read_export_cost(client,*,billing_project,export_table,month_start,month_end,query_bytes_limit=10_000_000):
    if not re.fullmatch(r'[a-z][a-z0-9-]{4,62}',billing_project):raise BillingExportUnavailable('invalid billing project')
    if not re.fullmatch(r'[\w-]+\.[\w-]+\.[\w-]+',export_table):raise BillingExportUnavailable('invalid export table')
    if not isinstance(query_bytes_limit,int) or query_bytes_limit<1 or query_bytes_limit>10_000_000:
        raise BillingExportUnavailable('query bytes cap exceeded')
    try:
        from google.cloud import bigquery
        cfg=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter('project','STRING',billing_project),
            bigquery.ScalarQueryParameter('start','TIMESTAMP',month_start),
            bigquery.ScalarQueryParameter('end','TIMESTAMP',month_end)],
            maximum_bytes_billed=query_bytes_limit,use_query_cache=False)
        # Sum usage cost less credits, but do not interpret delayed export as fresh spend.
        sql=f'''SELECT currency, SUM(cost + IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c),0)) AS net_cost,
                MAX(export_time) AS latest_export
                FROM `{export_table}`
                WHERE project.id=@project AND usage_start_time>=@start AND usage_start_time<@end
                GROUP BY currency'''
        rows=list(client.query(sql,job_config=cfg).result())
    except Exception as exc:
        raise BillingExportUnavailable('billing export query failed') from exc
    if len(rows)!=1 or rows[0]['currency']!='GBP' or rows[0]['latest_export'] is None:
        raise BillingExportUnavailable('missing or ambiguous GBP export')
    value=Decimal(str(rows[0]['net_cost']))
    if not value.is_finite():raise BillingExportUnavailable('invalid billing amount')
    return {'net_cost_gbp':str(value),'latest_export':rows[0]['latest_export'],
            'authorises_launch':False,'source':'DELAYED_GCP_BILLING_EXPORT'}
