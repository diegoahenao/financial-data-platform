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
    CALL RAW.SP_INGEST_TRANSACTIONS();

-- Resume the task to enable the schedule
ALTER TASK TSK_BRONZE_MASTER RESUME;

