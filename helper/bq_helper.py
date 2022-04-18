import google.cloud.bigquery as bigquery
import google.cloud.storage as storage
from datetime import datetime as date
from datetime import timedelta
from time import sleep

SLEEP_TIME = 60 * 2

def upload_to_regular_export_storage(bucket_name, table_name, delta = 1, sharded= False):
    today = date.today()
    n_day_ago = today - timedelta(days=delta)
    export_date = n_day_ago.strftime("%Y-%m-%d")
    bq_client = bigquery.Client.from_service_account_json('/home/mark/helper/python_api.json')
    project = "api-project-743945597108"
    dataset_id = "regular_export"
    if sharded:
        extra_symbol = '_*'
    else:
        extra_symbol = ''
    destination_uri = "gs://{bucket}/{date}/{date}{extra_symbol}.json".format(bucket=bucket_name,
                                                                              extra_symbol=extra_symbol,
                                                                              date=export_date)
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = 'NEWLINE_DELIMITED_JSON'

    extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        location="US",
        job_config=job_config,
    )
    return extract_job#.result()

bq_client = bigquery.Client.from_service_account_json('/home/mark/helper/python_api.json')

table_name = 'regular_export_from_ga_sessions_3'
regular_storage = 'regular-export-from-ga_sessions'


query = f'''
insert into `api-project-743945597108.regular_export.{table_name}`
(
  visitorId,
  visitNumber,
  visitId,
  visitStartTime,
  date,
  totals,
  trafficSource,
  device,
  geoNetwork,
  customDimensions,
  hits,
  fullVisitorId,
  userId ,
  clientId ,
  channelGrouping ,
  socialEngagementType,
  privacyInfo
) 
select 
*
replace
( format_date("%Y-%m-%d", date_sub(current_date(), interval 2 day)) AS date)
from 
`api-project-743945597108.95066265.ga_sessions_*`
where
_TABLE_SUFFIX = format_date('%Y%m%d',date_sub(current_date(), interval 2 day))
'''


query_truncate = f'truncate table `api-project-743945597108.regular_export.{table_name}`'

print(f'Truncating {table_name}')
resp = bq_client.query(query_truncate)
sleep(3)
print(f'Updating {table_name}')
resp = bq_client.query(query)
sleep(SLEEP_TIME)

job = upload_to_regular_export_storage(bucket_name=regular_storage,table_name=table_name, delta=0, sharded=True)
print(f'Loaded {table_name} to regular storage {regular_storage}')

table_name = 'regular_export_from_ga_sessions_intraday_4'
regular_storage = 'regular-export-from-ga_sessions_intraday'

query = f'''
insert into `api-project-743945597108.regular_export.{table_name}`
(
  visitorId,
  visitNumber,
  visitId,
  visitStartTime,
  date,
  totals,
  trafficSource,
  device,
  geoNetwork,
  customDimensions,
  hits,
  fullVisitorId,
  userId ,
  clientId ,
  channelGrouping ,
  socialEngagementType,
  privacyInfo
) select 
*
replace
( format_date("%Y-%m-%d", date_sub(current_date(), interval 1 day)) AS date)
from 
`api-project-743945597108.95066265.ga_sessions_intraday_*`
where
_TABLE_SUFFIX = format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
'''


query_truncate = f'truncate table `api-project-743945597108.regular_export.{table_name}`'

resp = bq_client.query(query_truncate)
print(f'Truncating {table_name}')
sleep(3)
resp = bq_client.query(query)
print(f'Updating {table_name}')
sleep(SLEEP_TIME)
job = upload_to_regular_export_storage(bucket_name=regular_storage,table_name=table_name, delta=0, sharded=True)
print(f'Loaded {table_name} to regular storage {regular_storage}')

table_name = 'regular_export_from_appsflyer_3'
query_truncate = f'truncate table `api-project-743945597108.regular_export.{table_name}`'
regular_storage = 'regular-export-from-appsflyer'

query = f'''
insert into `api-project-743945597108.regular_export.{table_name}`
select 
cast(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)as string) as date,
attributed_touch_type,
attributed_touch_time,
install_time,
event_time,
event_name,
event_value,
event_revenue,
event_revenue_currency,
event_revenue_usd,
af_cost_model,
af_cost_value,
af_cost_currency,
event_source,
is_receipt_validated,
af_prt,
media_source,
af_channel,
af_keywords,
install_app_store,
campaign,
af_c_id,
af_adset,
af_adset_id,
af_ad,
af_ad_id,
af_ad_type,
af_siteid,
af_sub_siteid,
af_sub1,
af_sub2,
af_sub3,
af_sub4,
af_sub5,
contributor_1_touch_type,
contributor_1_touch_time,
contributor_1_af_prt,
contributor_1_match_type,
contributor_1_media_source,
contributor_1_campaign,
contributor_2_touch_type,
contributor_2_touch_time,
contributor_2_af_prt,
contributor_2_media_source,
contributor_2_campaign,
contributor_2_match_type,
contributor_3_touch_type,
contributor_3_touch_time,
contributor_3_af_prt,
contributor_3_media_source,
contributor_3_campaign,
contributor_3_match_type,
region,
country_code,
state,
city,
postal_code,
dma,
ip,
wifi,
operator,
carrier,
language,
appsflyer_id,
customer_user_id,
android_id,
advertising_id,
imei,
idfa,
idfv,
amazon_aid,
device_type,
device_category,
platform,
os_version,
app_version,
sdk_version,
app_id,
app_name,
bundle_id,
is_retargeting,
retargeting_conversion_type,
cast(is_primary_attribution as string),
af_attribution_lookback,
af_reengagement_window,
match_type,
user_agent,
http_referrer,
original_url,
gp_referrer,
gp_click_time,
gp_install_begin,
gp_broadcast_referrer,
custom_data,
network_account_id,
keyword_match_type,
blocked_reason,
blocked_reason_value,
blocked_reason_rule,
blocked_sub_reason,
af_web_id,
web_event_type,
device_download_time,
deeplink_url,
oaid,
null as ad_unit,
null as app_type,
null as att,
null as campaign_type,
null as conversion_type,
null as custom_dimension,
null as detection_date,
null as device_model,
null as fraud_reason,
null as fraud_sub_reason,
null as impressions,
null as is_lat,
null as is_organic,
null as keyword_id,
null as mediation_network,
null as monetization_network,
null as placement,
null as rejected_reason,
null as segment,
null as validation_reason_value
from `api-project-743945597108.Appsflyer.raw` where DATE(event_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)


union all 

select  
cast(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)as string) as date,
attributed_touch_type,
attributed_touch_time,		
install_time,
event_time,
event_name,
event_value,
event_revenue,		
event_revenue_currency,		
event_revenue_usd,		
af_cost_model,		
af_cost_value,		
af_cost_currency,		
event_source,		
is_receipt_validated,		
af_prt,		
media_source,		
af_channel,		
af_keywords,		
install_app_store,		
campaign,		
af_c_id,		
af_adset,		
af_adset_id,		
af_ad,		
af_ad_id,		
af_ad_type,		
af_siteid,		
af_sub_siteid,		
af_sub1,		
af_sub2,		
af_sub3,		
af_sub4,		
af_sub5,		
contributor_1_touch_type,		
contributor_1_touch_time,		
contributor_1_af_prt,		
contributor_1_match_type,		
contributor_1_media_source,		
contributor_1_campaign,		
contributor_2_touch_type,		
contributor_2_touch_time,		
contributor_2_af_prt,		
contributor_2_media_source,		
contributor_2_campaign,		
contributor_2_match_type,		
contributor_3_touch_type,		
contributor_3_touch_time,		
contributor_3_af_prt,		
contributor_3_media_source,		
contributor_3_campaign,		
contributor_3_match_type,		
region,		
country_code,		
state,		
city,		
postal_code,		
dma,		
ip,		
wifi,
operator,		
carrier,		
language,		
appsflyer_id,		
customer_user_id,		
android_id,		
advertising_id,		
imei,		
idfa,		
idfv,		
amazon_aid,		
device_type,		
device_category,		
platform,		
os_version,		
app_version,		
sdk_version,		
app_id,		
app_name,		
bundle_id,		
is_retargeting,
retargeting_conversion_type,		
is_primary_attribution,		
af_attribution_lookback,		
af_reengagement_window,		
match_type,
user_agent,
http_referrer,
original_url,
gp_referrer,
gp_click_time,
gp_install_begin,		
gp_broadcast_referrer,		
custom_data,		
network_account_id,		
keyword_match_type,		
blocked_reason,		
blocked_reason_value,		
blocked_reason_rule,		
blocked_sub_reason,		
af_web_id,		
web_event_type,		
device_download_time,		
deeplink_url,		
oaid,		
ad_unit,
null as app_type,
null as att,
null as campaign_type,
null as conversion_type,
null as custom_dimension,
null as detection_date,
null as device_model,
null as fraud_reason,
null as fraud_sub_reason,
impressions,
null as is_lat,
null as is_organic,
null as keyword_id,
mediation_network,
monetization_network,
placement,
null as rejected_reason,
segment,
null as validation_reason_value
from `api-project-743945597108.Appsflyer_SberBusiness.events` where DATE(event_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)

union all 

select  
cast(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)as string) as date,
attributed_touch_type,
attributed_touch_time,
install_time,
event_time,
event_name,
event_value,
event_revenue,
event_revenue_currency,
event_revenue_usd,
af_cost_model,
af_cost_value,
af_cost_currency,
event_source,
is_receipt_validated,
af_prt,
media_source,
af_channel,
af_keywords,
install_app_store,
campaign,
af_c_id,
af_adset,
af_adset_id,
af_ad,
af_ad_id,
af_ad_type,
af_siteid,
af_sub_siteid,
af_sub1,
af_sub2,
af_sub3,
af_sub4,
af_sub5,
contributor_1_touch_type,
contributor_1_touch_time,
contributor_1_af_prt,
contributor_1_match_type,
contributor_1_media_source,
contributor_1_campaign,
contributor_2_touch_type,
contributor_2_touch_time,
contributor_2_af_prt,
contributor_2_media_source,
contributor_2_campaign,
contributor_2_match_type,
contributor_3_touch_type,
contributor_3_touch_time,
contributor_3_af_prt,
contributor_3_media_source,
contributor_3_campaign,
contributor_3_match_type,
region,
country_code,
state,
city,
postal_code,
dma,
ip,
wifi,
operator,
carrier,
language,
appsflyer_id,
customer_user_id,
android_id,
advertising_id,
imei,
idfa,
idfv,
amazon_aid,
device_type,
device_category,
platform,
os_version,
app_version,
sdk_version,
app_id,
app_name,
bundle_id,
is_retargeting,
retargeting_conversion_type,
is_primary_attribution,
af_attribution_lookback,
af_reengagement_window,
match_type,
user_agent,
http_referrer,
original_url,
gp_referrer,
gp_click_time,
gp_install_begin,
gp_broadcast_referrer,
custom_data,
network_account_id,
keyword_match_type,
blocked_reason,
blocked_reason_value,
blocked_reason_rule,
blocked_sub_reason,
af_web_id,
web_event_type,
device_download_time,
deeplink_url,
oaid,
ad_unit,
app_type,
att,
campaign_type,
conversion_type,
custom_dimension,
detection_date,
device_model,
fraud_reason,
fraud_sub_reason,
impressions,
is_lat,
is_organic,
keyword_id,
mediation_network,
monetization_network,
placement,
rejected_reason,
segment,
validation_reason_value
from `api-project-743945597108.Appsflyer_SberInvestor.events` where DATE(event_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
'''


print(f'Truncating {table_name}')
resp = bq_client.query(query_truncate)
sleep(3)
print(f'Updating {table_name}')
resp = bq_client.query(query)
sleep(SLEEP_TIME)

job = upload_to_regular_export_storage(bucket_name=regular_storage,table_name=table_name, delta=0, sharded=True)
print(f'Loaded {table_name} to regular storage {regular_storage}')

table_name = 'regular_export_from_dcm_3'

query_truncate = f'truncate table `api-project-743945597108.regular_export.{table_name}`'
regular_storage = 'regular-export-from-dcm'

query = f'''insert into `api-project-743945597108.regular_export.{table_name}` 
select 
conversion_id,
impression_count,
click_count,
days_since_attributed_interaction,
days_since_first_interaction,
activity_time,
interaction_time,
interaction_number,
creative_id,
creative,
ad,
ad_id,
placement,
placement_id,
interaction_type,
activity_count, 
'string' as floodlight_variable_dimension1,   
floodlight_variable_dimension2,
'string' as floodlight_variable_dimension3,  
'string' as floodlight_variable_dimension4,  
floodlight_variable_dimension5,
floodlight_variable_dimension6,
'string' as floodlight_variable_dimension7, 
'string' as floodlight_variable_dimension8,  
'string' as floodlight_variable_dimension9,  
'string' as floodlight_variable_dimension10,  
'string' as floodlight_variable_dimension11,
substr(activity_time, 1, strpos(activity_time, ' ') - 1) as date 
FROM `api-project-743945597108.DCM.ClientId_SberBank_cm360_1224329_750993086_*` 
where
_TABLE_SUFFIX = format_date('%Y%m%d',date_sub(current_date(), interval 2 day))   
'''


resp = bq_client.query(query_truncate)
print(f'Truncating {table_name}')
sleep(3)
resp = bq_client.query(query)
print(f'Updating {table_name}')
sleep(SLEEP_TIME)
job = upload_to_regular_export_storage(bucket_name=regular_storage,table_name=table_name, delta=0, sharded=True)
print(f'Loaded {table_name} to regular storage {regular_storage}')


from google.cloud import bigquery


"""
table_name = "regular_export.regular_export_from_facebook_2"

query_truncate = f'truncate table `api-project-743945597108.regular_export.{table_name}`'

print(f'Truncating {table_name}')
resp = bq_client.query(query_truncate)


print('Uploading facebook data from adv_connectors_data to big query and redirecting to storage')
bq_client = bigquery.Client.from_service_account_json('/home/mark/bq_loader/python_api.json')
today = date.now()
day_ago = today - timedelta(days=1)
export_date = day_ago.strftime("%Y-%m-%d")
table_id = "regular_export.regular_export_from_facebook_2"
#удалены: url_tags
cols = '''account_currency,account_id,account_name,action_values,actions,ad_id,ad_name,adset_id,adset_name,attribution_setting,buying_type,campaign_id,campaign_name,canvas_avg_view_percent,canvas_avg_view_time,catalog_segment_value,clicks,conversion_rate_ranking,conversion_values,conversions,converted_product_quantity,converted_product_value,cost_per_action_type,cost_per_conversion,cost_per_estimated_ad_recallers,cost_per_inline_link_click,cost_per_inline_post_engagement,cost_per_outbound_click,cost_per_thruplay,cost_per_unique_action_type,cost_per_unique_click,cost_per_unique_inline_link_click,cost_per_unique_outbound_click,cpc,cpm,cpp,ctr,date_start,date_stop,engagement_rate_ranking,estimated_ad_recall_rate,estimated_ad_recallers,frequency,full_view_impressions,full_view_reach,impressions,inline_link_click_ctr,inline_link_clicks,inline_post_engagement,instant_experience_clicks_to_open,instant_experience_clicks_to_start,instant_experience_outbound_clicks,mobile_app_purchase_roas,objective,optimization_goal,outbound_clicks,outbound_clicks_ctr,purchase_roas,qualifying_question_qualify_answer_rate,quality_ranking,reach,social_spend,spend,video_30_sec_watched_actions,video_avg_time_watched_actions,video_p100_watched_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,video_play_actions,video_play_curve_actions,website_ctr,website_purchase_roas,url_tags,date'''
cols = cols.split(',')

job_config = bigquery.LoadJobConfig(
    schema=[bigquery.SchemaField(col, "STRING") for col in cols],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://adv_connectors_data/fb/{date}/{date}.csv".format(date=export_date)
job_config.ignore_unknown_values = True
load_job = bq_client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

#destination_table = bq_client.get_table(table_id)  # Make an API request.
#print("Loaded {} rows.".format(destination_table.num_rows))

upload_to_regular_export_storage('regular-export-from-cost_facebook', 'regular_export_from_facebook_2', delta=0)


from google.cloud import bigquery

table_name = "regular_export.regular_export_from_vkontakte_2"

query_truncate = f'truncate table `api-project-743945597108.regular_export.{table_name}`'

print(f'Truncating {table_name}')
resp = bq_client.query(query_truncate)



print('Uploading vk data from adv_connectors_data to big query and redirecting to storage')
day_ago = today - timedelta(days=1)
export_date = day_ago.strftime("%Y-%m-%d")
table_id = "regular_export.regular_export_from_vkontakte_2"
# удалены: id, date
cols = 'id,type,campaign_id,date,spent,impressions,clicks,reach,join_rate,uniq_views_count,ctr,effective_cost_per_click,effective_cost_per_mille,effective_cpf,effective_cost_per_message,message_sends,video_plays_unique_started,video_plays_unique_3_seconds,video_plays_unique_25_percents,video_plays_unique_50_percents,video_plays_unique_75_percents,video_plays_unique_100_percents,conversion_count,conversion_sum,conversion_roas,conversion_cr'
cols = cols.split(',')

job_config = bigquery.LoadJobConfig(
    schema=[bigquery.SchemaField(col, "STRING") for col in cols],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://adv_connectors_data/vk/{date}/{date}.csv".format(date=export_date)
job_config.ignore_unknown_values = True
load_job = bq_client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()
upload_to_regular_export_storage('regular-export-from-cost_vkontakte', 'regular_export_from_vkontakte_2', delta=0)

from google.cloud import bigquery


table_name = "regular_export.regular_export_from_my_target_2"

query_truncate = f'truncate table `api-project-743945597108.regular_export.{table_name}`'

print(f'Truncating {table_name}')
resp = bq_client.query(query_truncate)



print('Uploading my target data from adv_connectors_data to big query and redirecting to storage')
day_ago = today - timedelta(days=1)
export_date = day_ago.strftime("%Y-%m-%d")
table_id = "regular_export.regular_export_from_my_target_2"
# удалены: id, date
cols = "id,campaign_id,date,ad_name,campaign_name,base_shows,base_clicks,base_goals,base_spent,base_cpm,base_cpc,base_cpa,base_ctr,base_cr,events_opening_app,events_opening_post,events_moving_into_group,events_clicks_on_external_url,events_launching_video,events_comments,events_joinings,events_likes,events_shares,events_votings,events_sending_form,uniques_reach,uniques_total,uniques_increment,uniques_frequency,uniques_frequency_total,video_started,video_paused,video_resumed_after_pause,video_fullscreen_on,video_fullscreen_off,video_sound_turned_off,video_sound_turned_on,video_viewed_10_seconds,video_viewed_25_percent,video_viewed_50_percent,video_viewed_75_percent,video_viewed_100_percent,video_viewed_10_seconds_rate,video_viewed_25_percent_rate,video_viewed_50_percent_rate,video_viewed_75_percent_rate,video_viewed_100_percent_rate,video_depth_of_view,video_viewed_10_seconds_cost,video_viewed_25_percent_cost,video_viewed_50_percent_cost,video_viewed_75_percent_cost,video_viewed_100_percent_cost,viral_impressions,viral_reach,viral_total,viral_increment,viral_frequency,viral_opening_app,viral_opening_post,viral_moving_into_group,viral_clicks_on_external_url,viral_launching_video,viral_comments,viral_joinings,viral_likes,viral_shares,viral_votings,viral_sending_form,carousel_slide_1_clicks,carousel_slide_1_shows,carousel_slide_2_clicks,carousel_slide_2_shows,carousel_slide_3_clicks,carousel_slide_3_shows,carousel_slide_4_clicks,carousel_slide_4_shows,carousel_slide_5_clicks,carousel_slide_5_shows,carousel_slide_6_clicks,carousel_slide_6_shows,carousel_slide_1_ctr,carousel_slide_2_ctr,carousel_slide_3_ctr,carousel_slide_4_ctr,carousel_slide_5_ctr,carousel_slide_6_ctr,ad_offers_offer_postponed,ad_offers_upload_receipt,ad_offers_earn_offer_rewards,playable_playable_game_open,playable_playable_game_close,playable_playable_call_to_action,tps_tps,tps_tpd,moat_impressions,moat_in_view,moat_never_focused,moat_never_visible,moat_never_50_perc_visible,moat_never_1_sec_visible,moat_human_impressions,moat_impressions_analyzed,moat_in_view_percent,moat_human_and_viewable_perc,moat_never_focused_percent,moat_never_visible_percent,moat_never_50_perc_visible_percent,moat_never_1_sec_visible_percent,moat_in_view_diff_percent,moat_active_in_view_time,moat_attention_quality,romi_value,romi_romi,romi_adv_cost_share"
cols = cols.split(',')

job_config = bigquery.LoadJobConfig(
    schema=[bigquery.SchemaField(col, "STRING") for col in cols],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://adv_connectors_data/mt/{date}/{date}.csv".format(date=export_date)
job_config.ignore_unknown_values = True
load_job = bq_client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()
upload_to_regular_export_storage('regular-export-from-cost_my_target', 'regular_export_from_my_target_2', delta=0)
"""