select  *  from ( select  assets.is_current asset_currrent, Asset_ID,SN_System_ID, systems.name system_name,systems.is_current, 'Number' as attribute_type, CI_Number sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID
      union

      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'URL' as attribute_type, url sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID


      union

      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'Export Control' as attribute_type, Export_Control sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      union

      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'Legal Hold' as attribute_type, Legal_Hold sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      union

      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'Data Sensitivity' as attribute_type, APM_Data_Sensitivity sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      union

      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'Disaster Recovery Required' as attribute_type, Disaster_Recovery_Gap sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      union


      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'Records Retention' as attribute_type, Records_Retention sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      union


      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'Business Owner' as attribute_type, Owned_By sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      union

      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'IT Application Owner' as attribute_type, IT_Owner sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      union

      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'Application Contact' as attribute_type, Supported_By sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      union

      select assets.is_current asset_currrent, Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'Description' as attribute_type, Description sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      union

      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'Application Status' as attribute_type, Install_Status sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      union

      select assets.is_current asset_currrent,Asset_ID,SN_System_ID, systems.name system_name,systems.is_current,'Business Criticality' as attribute_type, Business_Criticality sn_value from
      (
      select * from servicenow.servicenow_cmbd_ci_business_app
      union
      select * from servicenow.servicenow_cmbd_ci_service
      union
      select * from servicenow.servicenow_cmbd_ci_service_discovered
      ) systems
      join collibra.collibra_assets assets on assets.Name = systems.SN_System_ID

      ) system_attributes


      left join
      (select Attribute_Name, attribute_id, Attribute_Value collibra_value, Parent_Asset_ID,  Attribute_Type_ID
      from collibra.collibra_attributes collibra_attributes
      where collibra_attributes.is_current = 1)
      collibra_attributes
      on collibra_attributes.Parent_Asset_ID = system_attributes.Asset_ID
      and attribute_type = Attribute_Name
      where is_current = 1
      and asset_currrent = 1
      and
      (
      collibra_value != sn_value
      or collibra_value is null
      or sn_value is null
      )
      and SN_System_ID not in

      ( select
      [SN_System_ID]
      from servicenow.servicenow_cmbd_ci_business_app
      as sn_business_apps
      left join
      (select * from collibra.collibra_assets where is_current=1 and
       Domain_ID in ('{0}', '{1}')) collibra_assets on SN_System_ID =
      collibra_assets.name where collibra_assets.Name is null and sn_business_apps.is_current = 1

      Union

      select
      [SN_System_ID]
      from servicenow.servicenow_cmbd_ci_service_discovered
      as sn_service_discovered
      left Join
      (select * from collibra.collibra_assets where is_current=1 and
       Domain_ID in ('{0}', '{1}')) collibra_assets on  SN_System_ID =
      collibra_assets.name where collibra_assets.Name is null and sn_service_discovered.is_current = 1

      Union

      select
      [SN_System_ID]
      from servicenow.servicenow_cmbd_ci_service
      as sn_services
      left Join
      (select * from collibra.collibra_assets where is_current=1 and
      Domain_ID in ('{0}', '{1}')) collibra_assets on  SN_System_ID =
      collibra_assets.name where collibra_assets.Name is null and sn_services.is_current = 1
      )
       and sn_value not in ('None', 'Unknown')