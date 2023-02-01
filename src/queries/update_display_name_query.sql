/* 
Find mismatching display names
*/
	  
select  Asset_ID, system_name sn_value, collibra_display_name collibra_value from(

select Name system_name, is_current, SN_System_ID from servicenow.servicenow_cmbd_ci_business_app where is_current = 1
union
select name system_name, is_current,SN_System_ID from servicenow.servicenow_cmbd_ci_service where is_current = 1
)
systems

join 
(select is_current asset_current, asset_id, name collibra_name,
display_name collibra_display_name from collibra.collibra_assets
where is_current = 1) 
assets

on assets.collibra_name = systems.SN_System_ID

where system_name != collibra_display_name