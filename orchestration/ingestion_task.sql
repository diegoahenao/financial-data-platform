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
BEGIN
    CALL RAW.SP_INGEST_TRANSACTIONS();
    CALL RAW.SP_INGEST_CUSTOMERS();
    CALL RAW.SP_INGEST_PAYMENTS();
    CALL RAW.SP_INGEST_ORDERS();
    CALL RAW.SP_INGEST_PRODUCTS();
END;

ALTER TASK TSK_BRONZE_MASTER RESUME;

