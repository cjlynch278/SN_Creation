/* 
Find mismatching display names
*/
                
select  Asset_ID, system_name sn_value, collibra_display_name collibra_value from(

select Name system_name, is_current, SN_System_ID from servicenow.{3} where is_current = 1 and name is not null

)
systems

join 
(select is_current asset_current, asset_id, name collibra_name,
display_name collibra_display_name from collibra.collibra_assets
where is_current = 1 and Domain_ID in ('{0}', '{1}')) 
assets

on assets.collibra_name = systems.SN_System_ID
where system_name != collibra_display_name
