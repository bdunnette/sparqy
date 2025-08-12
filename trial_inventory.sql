SELECT
    tr.TRIAL_CODE
    , cr.SUBJECTID
    , cr.FIRST_NAME as [LABID]
    , sc.CONTAINERID as [CID]
    , sc.SAMPLETYPE
    , cc.PRESERVATIVE
    , it.AMOUNTLEFT
    , it.AMOUNT_UNIT_CODE
    , it.THAWCOUNT
    , inv.EXTERNAL_CODE as [Seq]
    , (
        CASE
            WHEN freezer.LOCATIONCODE IS NOT NULL THEN freezer.LOCATION_NAME
            ELSE shelf.LOCATION_NAME
        END
    ) AS [Freezer]
    , (
        CASE
            WHEN freezer.LOCATIONCODE IS NOT NULL THEN shelf.LOCATION_NAME
            ELSE rack.LOCATION_NAME
        END
    ) AS [Shelf]
    , (
        CASE
            WHEN freezer.LOCATIONCODE IS NOT NULL THEN rack.LOCATION_NAME
            ELSE NULL
        END
    ) AS [Rack]
    , cr.FOLDERNO as [Order # (Accession)]
    , cr.DATE_COLLECTED
    , cr.DATE_RECEIVED
    , PARENT_CONTAINER.EXTERNAL_CODE as [Box Name]
    , PARENT_CONTAINER.INVENTORY_CODE as [Box Code]
    , POSITION.ORDINAL AS [Box Pos]
    , sc.RECEIVEDCONDITION
    , inv_meta.field16 as [Sample Condition]
    , inv_meta.FIELD01 as [Comments]
    , tms.MSDESCRIPTION as [MS Visit]
    , tms.TRIAL_MASTER_SCHEDULE_ID as [TMS_ID]
    , tetp.CODE as [TP Code]
    , tetp.VISIT as [TP Visit]
    , tetp.DESCRIPTION as [TP Desc]
    , te.ELEMENT_NAME [TE Name]
    , cr_meta.FIELD03 as [NickName]
    , cr_meta.FIELD02 as [CRVisit]
    , it.CONSENT_TYPE
    , rack.LOCATIONCODE as [Rack Code]
    , shelf.SORTER as [ShelfSort]
    , rack.SORTER as [RackSort]
    , sc.CONTAINERMATCODE
    , PARENT_CONTAINER.MATCODE
    , it.CONTAINER_POS_Y
    , m.CONTAINER_AXIS_Y_SIZE
    , it.CONTAINER_POS_X
    , m.CONTAINER_AXIS_X_SIZE
    , cr.LAST_NAME as [Study (Lastname)]
    , cr.RASCLIENTID as [Study Site]
    , rack.LONGNAME
    , rack.LONGCODE
    , ROOMS.ROOM_NAME
    , ROOMS.ROOM_CODE
    , BUILDINGS.BUILDING_NAME as BUILDING_NAME
    , BUILDINGS.BUILDING_CODE as BUILDING_CODE
    , cr.ArcStatus as [ARCHIVE_STATUS]
    , inv.INVENTORYID as [VIAL_CONTAINER_INV_ID]
    , PARENT_CONTAINER.INVENTORYID as [PARENT_CONTAINER_INV_ID]
    , cr.ORIGREC as [CR_ORIGREC]
FROM INVENTORY_VLA inv
    inner join CENTRALRECEIVING_VLA cr on cr.EXTERNAL_ID = inv.EXTERNAL_ID
    inner join samplecontainers_VLA sc on sc.inventoryid = inv.inventoryid
    inner join INVENTORY_TRANSACTIONS_VLA it on it.INVENTORYID = inv.INVENTORYID and it.FLAG_CURRENT = 1
    outer apply (select pi.MATCODE, pi.EXTERNAL_CODE, pi.INVENTORYID, pi.INVENTORY_CODE
    from INVENTORY pi
    where pi.INVENTORYID = it.CONTAINER_INVENTORYID and pi.INVENTORYID <> inv.INVENTORYID) as PARENT_CONTAINER
    outer apply (select pit.LOCATIONCODE, pit.COMMENTS
    from inventory_transactions pit
    where pit.inventoryid = PARENT_CONTAINER.inventoryid and pit.flag_current = 1) as PARENT_CONTAINER_IT
    inner join METADATA cr_meta on cr_meta.ID = cr.METADATA_GUID
    inner join METADATA inv_meta on inv_meta.ID = inv.METADATA_GUID
    inner join TRIAL_MASTER_SCHEDULE tms on tms.TRIAL_MASTER_SCHEDULE_ID = cr.TRIAL_MASTER_SCHEDULE_ID
    LEFT JOIN TRIAL_ELEMENT_TIMEPOINT tetp on tetp.TRIAL_ELEMENT_TP_ID = tms.TRIAL_ELEMENT_TP_ID
    LEFT JOIN TRIAL_ELEMENT te on te.TRIAL_ELEMENT_ID = tetp.TRIAL_ELEMENT_ID
    inner join TRIAL tr on tms.TRIAL_ID = tr.TRIAL_ID
    left join MATERIALS m on PARENT_CONTAINER.MATCODE = m.MATCODE
    left join CONTAINERS_CONDITION cc on cc.CONTCODE = sc.CONTCODE
    outer apply	(select((it.CONTAINER_POS_Y - 1) * m.CONTAINER_AXIS_X_SIZE + it.CONTAINER_POS_X) as ORDINAL) as POSITION
    -- first-level location (rack)
    LEFT JOIN LOCATIONS rack ON it.LOCATIONCODE = rack.LOCATIONCODE
    -- second-level location (shelf)
    LEFT JOIN LOCATIONS shelf ON rack.PARENT_LOCATION_CODE = shelf.LOCATIONCODE
    -- third-level location (freezer)
    LEFT JOIN LOCATIONS freezer ON shelf.PARENT_LOCATION_CODE = freezer.LOCATIONCODE
    LEFT JOIN ROOMS on freezer.ROOM_ID = ROOMS.ROOM_ID
    LEFT JOIN BUILDINGS on ROOMS.BUILDING_ID = BUILDINGS.BUILDING_ID
WHERE
    cr.LAST_NAME = '{TRIAL_CODE}'
