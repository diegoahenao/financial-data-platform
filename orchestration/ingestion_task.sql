/********************************************************************************
* Task Name:    TSK_BRONZE_MASTER
* Description:  Root orchestrator for the Bronze (Landing) Layer. 
* This task triggers the ingestion procedures for all five 
* core business entities from Azure Blob Storage.
* Warehouse:    COMPUTE_WH (Size: XSMALL)
* Schedule:     Runs every 60 minutes
* Author:       Diego
* Date:         2026-03-16
********************************************************************************/

CREATE OR REPLACE TASK TSK_BRONZE_MASTER
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '60 MINUTE'
AS
$$
BEGIN
    -- 1. Ingest Transactional Data (XML, TXT, JSON)
    CALL RAW.SP_INGEST_TRANSACTIONS();
    
    -- 2. Ingest Master Data (Customers from CSV)
    CALL RAW.SP_INGEST_CUSTOMERS();
    
    -- 3. Ingest Payment Data (CSV logs)
    CALL RAW.SP_INGEST_PAYMENTS();
    
    -- 4. Ingest Order Records (CSV headers)
    CALL RAW.SP_INGEST_ORDERS();
    
    -- 5. Ingest Product Catalog (CSV pricing)
    CALL RAW.SP_INGEST_PRODUCTS();
END;
$$;

-- Resume the task to enable the schedule
ALTER TASK TSK_BRONZE_MASTER RESUME;

