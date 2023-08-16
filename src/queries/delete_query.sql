select assets_to_change.is_current, Attribute_ID, Name, Asset_ID, Attribute_Name, Attribute_Value, Display_Name
     from (  select assets.is_current, Name, Display_Name, SN_System_ID, Asset_ID from (select * from collibra.collibra_assets where is_current=1 and
     Domain_ID in ('{0}', '{1}')  ) assets


     left join
     ( select SN_System_ID, is_current from servicenow.{2}

     ) snow_apps on snow_apps.SN_System_ID = assets.name
     and   snow_apps.is_current = 1
     where SN_System_ID IS NULL
     ) assets_to_change
     left join collibra.collibra_attributes attributes on Parent_Asset_ID = Asset_ID
	 and attributes.is_current = 1
     and attributes.attribute_name = 'Application Status'
     where attributes.Attribute_Value != 'Retired-Decommissioned'