WITH PublishDateCTE AS (
    SELECT 
        CASE
            WHEN rpd.[PublishDate] > 0 THEN rpd.[PublishDate]
            WHEN retpd.[PublishedDatePKS] > 0 THEN CONVERT(DATE, retpd.[PublishedDatePKS])
            WHEN rpd.[AddedDate] > 0 THEN CONVERT(Date, DateAdd(Hour, 7, rpd.[AddedDate]))
            WHEN rh.[RequestDate] > 0 THEN CONVERT(Date, DateAdd(Hour, 7, rh.[RequestDate]))
            ELSE CONVERT(Date, DateAdd(Hour, 7, BinLogPID.[CompletedDateTime]))
        END AS PublishedDate,
        BinLogPID.[Id] Id,
        wh.[Id] as WarehouseId, 
        wh.[WarehouseName] WarehouseName,
        rh.[TransactionId] TransactionId,
        rh.[ExternalRefId] ExternalRefId,
        op.[PlacementTypeCode] PlacementTypeCode,
        wo.[RequestTypeCode] RequestTypeCode,
        sku.[Id] SKUId,
        sku.[ItemName] SKUName,
        CASE 
            WHEN BinLogPID.[MutateQty] > 0 
            THEN BinLogPID.[MutateQty] 
            ELSE 0 
        END as Kredit,
        CASE
            WHEN BinLogPID.[MutateQty] < 0
            THEN BinLogPID.[MutateQty]
            ELSE 0
        END as Debit,
        BinLogPID.[MutateQty] MutateQty,
        sku.[SKUCode] SKUCode,
        sku.[MainUOMCode] UOM,
        DateAdd(Hour, 7, BinLogPID.[CompletedDateTime]) CompletedDateTime,
        wo.[OrderActionCode] OrderActionCode,
        rh.[Id] RequestHeaderId,
        rpd.[PublishDate] CheckPublishedDate
    FROM {Bin_Location_Stock_Log_PID} BinLogPID 
    INNER JOIN {Order_Placement} op on BinLogPID.[Id] = op.[Id] 
    INNER JOIN {Work_Order_Detail} wod on op.[WorkOrderDetailID] = wod.[Id] 
    INNER JOIN {Work_Order} wo on wo.[ID] = wod.[WorkOrderID] 
    INNER JOIN {Warehouse} wh on wo.[WarehouseID] = wh.[Id]
    INNER JOIN {SKU} sku on sku.[Id] = wod.[SKUId] 
    LEFT OUTER JOIN (
        {Request_Header} rh 
        LEFT OUTER JOIN {RequestPublishDate} rpd ON rpd.[RequestHeaderId] = rh.[Id]
        LEFT OUTER JOIN {Return_Published_Date} retpd ON retpd.[RequestHeaderIdPKS] = rh.[Id]
    ) 
        ON wod.[RequestHeaderID] = rh.[Id]
    WHERE sku.[TenantId] = @tenant_id
    AND BinLogPID.[CompletedDateTime] >= @begin_date AND BinLogPID.[CompletedDateTime] <= @end_date
)
SELECT 
    PublishedDate,
    Id,
    WarehouseId,
    WarehouseName,
    TransactionId,
    ExternalRefId,
    SKUId,
    SKUName,
    Kredit,
    Debit,
    SUM(MutateQty) OVER (PARTITION BY WarehouseId, SKUId ORDER BY Id) as Mutasi,
    SUM(MutateQty) OVER (PARTITION BY WarehouseId, SKUId, PlacementTypeCode ORDER BY PublishedDate, Id) AS QtyInvMut,
    SKUCode,
    UOM,
    PlacementTypeCode,
    RequestTypeCode,
    CompletedDateTime,
    OrderActionCode,
    RequestHeaderId,
    CheckPublishedDate
FROM PublishDateCTE
ORDER BY PublishedDate