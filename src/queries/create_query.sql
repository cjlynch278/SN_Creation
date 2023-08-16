        select [SN_System_ID], {3}.[Name] asset_name ,[CI_Number],[Install_Status],
        [Business_Criticality] ,[URL],[Owned_By],[IT_Owner],[Supported_By],
        [Export_Control],[Legal_Hold],[APM_Data_Sensitivity],[Disaster_Recovery_Gap],[Records_Retention],
        [Description], [CI_Type]
        from servicenow.{3}
        as {4}
        left join
        (select * from collibra.collibra_assets where is_current=1 and
        Domain_ID in ('{0}', '{1}')) collibra_assets on SN_System_ID =
        collibra_assets.name where collibra_assets.Name is null and {3}.is_current = 1

        Union

        select
        [SN_System_ID], sn_services.[Name] asset_name,[CI_Number],[Install_Status],
        [Business_Criticality] ,[URL],[Owned_By],[IT_Owner],[Supported_By],
        [Export_Control],[Legal_Hold],[APM_Data_Sensitivity],[Disaster_Recovery_Gap],[Records_Retention], [Description]
        , [CI_Type]
        from servicenow.servicenow_cmbd_ci_service
        as sn_services
        left Join
        (select * from collibra.collibra_assets where is_current=1 and
        Domain_ID in ('{0}', '{1}')) collibra_assets on SN_System_ID =
        collibra_assets.name where collibra_assets.Name is null and sn_services.is_current = 1

