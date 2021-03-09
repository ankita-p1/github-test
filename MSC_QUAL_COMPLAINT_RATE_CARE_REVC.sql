{{ config(tags=["poc"]) }}
SELECT
distinct
  oa.ORDER_ID as MERCHANT_ORDER_ID
, coalesce(oa.item_id, oi.item_id) as MERCHANT_ITEM_ID
, oa.CASE_ID
, oa.ACTION_TYPE
, replace(oa.ACTION_REASON, 'Refund: ','') as ACTION_REASON
, date(oa.ACTION_TIMESTAMP) as ACTION_TIMESTAMP
, ct.CARE_CHANNEL
, array_to_string(array_agg(DISTINCT ct.TRANSCRIPT) OVER (PARTITION BY oa.ORDER_ID),', ') AS ALL_TRANSCRIPTS
from vistaprint.care.order_actions oa
left join vistaprint.care.case_transcripts ct
  ON oa.case_id = ct.case_id
inner join vistaprint.order_management.order_items oi
  on oa.order_id = oi.order_number
  and ifnull(oa.item_id,'') = (case when oa.item_id is null then '' else oi.item_id end)
  and oi.order_is_fake = 'false'
  and upper(oi.order_number) like 'VP\\_%' escape '\\'
  and upper(oi.order_number) not like 'VPM\\_%' escape '\\'
  and oi.item_states like '%Shipped%'
where oa.action_timestamp >= '2020-08-01'
and lower(oa.action_reason) not like '%internal%'
and upper(oa.order_id) like 'VP\\_%' escape '\\'
and upper(oa.order_id) not like 'VPM\\_%' escape '\\'



