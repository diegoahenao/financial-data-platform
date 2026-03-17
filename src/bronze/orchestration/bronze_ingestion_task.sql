/********************************************************************************
* Script: bronze_ingestion_task.sql
* Description: Orchestrates the ingestion from Azure to the 5 Landing Tables.
* Schedule: Runs every hour.
********************************************************************************/

USE ROLE ROLE_FIN_DATA_ENGINEER;
USE DATABASE FIN_DATA_DEV;
USE SCHEMA RAW;

-- 1. Create the Master Ingestion Task
CREATE OR REPLACE TASK TSK_BRONZE_MASTER_INGEST
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '60 MINUTE'
AS
BEGIN
    -- CLIENT A & C: Transactions (XML, TXT, JSON)
    COPY INTO RAW.TRANSACTIONS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (SELECT METADATA$FILENAME, $1 FROM @stg_finance_azure/client_a/)
    FILE_FORMAT = (FORMAT_NAME = 'ff_xml') PATTERN = '.*\.xml';

    COPY INTO RAW.TRANSACTIONS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (SELECT METADATA$FILENAME, TO_VARIANT($1) FROM @stg_finance_azure/client_a/)
    FILE_FORMAT = (FORMAT_NAME = 'ff_text') PATTERN = '.*\.txt';

    COPY INTO RAW.TRANSACTIONS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (SELECT METADATA$FILENAME, $1 FROM @stg_finance_azure/client_c/)
    FILE_FORMAT = (FORMAT_NAME = 'ff_json') PATTERN = '.*\.json';

    COPY INTO RAW.CUSTOMERS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (SELECT METADATA$FILENAME, OBJECT_CONSTRUCT(*) FROM @stg_finance_azure/)
    FILE_FORMAT = (FORMAT_NAME = 'ff_csv_header') PATTERN = '.*/customer.*\.csv';

    COPY INTO RAW.PAYMENTS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (SELECT METADATA$FILENAME, OBJECT_CONSTRUCT(*) FROM @stg_finance_azure/)
    FILE_FORMAT = (FORMAT_NAME = 'ff_csv_header') PATTERN = '.*/payments.*\.csv';

    COPY INTO RAW.ORDERS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (SELECT METADATA$FILENAME, OBJECT_CONSTRUCT(*) FROM @stg_finance_azure/)
    FILE_FORMAT = (FORMAT_NAME = 'ff_csv_header') PATTERN = '.*/order.*\.csv';

    COPY INTO RAW.PRODUCTS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (SELECT METADATA$FILENAME, OBJECT_CONSTRUCT(*) FROM @stg_finance_azure/)
    FILE_FORMAT = (FORMAT_NAME = 'ff_csv_header') PATTERN = '.*/product.*\.csv';
END;

-- 2. Resume the task (Snowflake tasks are created in 'SUSPENDED' state)
ALTER TASK TSK_BRONZE_MASTER_INGEST RESUME;