/********************************************************************************
* Script: load_subject_area_landing.sql
* Description: Routes all files (XML, JSON, TXT, CSV) into 5 Unified Tables.
********************************************************************************/

USE ROLE ROLE_FIN_DATA_ENGINEER;
USE DATABASE FIN_DATA_DEV;
USE SCHEMA RAW;

-- ==============================================================================
-- 1. TRANSACTIONS (XML, TXT from Client A | JSON from Client C)
-- ==============================================================================

-- Client A: XML
COPY INTO RAW.TRANSACTIONS_LANDING (SOURCE_PATH, RAW_CONTENT)
FROM (SELECT METADATA$FILENAME, $1 FROM @stg_finance_azure/client_a/)
FILE_FORMAT = (FORMAT_NAME = 'ff_xml') PATTERN = '.*\.xml';

-- Client A: TXT
COPY INTO RAW.TRANSACTIONS_LANDING (SOURCE_PATH, RAW_CONTENT)
FROM (SELECT METADATA$FILENAME, TO_VARIANT($1) FROM @stg_finance_azure/client_a/)
FILE_FORMAT = (FORMAT_NAME = 'ff_text') PATTERN = '.*\.txt';

-- Client C: JSON
COPY INTO RAW.TRANSACTIONS_LANDING (SOURCE_PATH, RAW_CONTENT)
FROM (SELECT METADATA$FILENAME, $1 FROM @stg_finance_azure/client_c/)
FILE_FORMAT = (FORMAT_NAME = 'ff_json') PATTERN = '.*\.json';

-- ==============================================================================
-- 2. MASTER DATA (CSV from both clients)
-- Note: We ingest CSV columns into a single VARIANT using OBJECT_CONSTRUCT
-- ==============================================================================

-- CUSTOMERS (Client A & Client C)
COPY INTO RAW.CUSTOMERS_LANDING (SOURCE_PATH, RAW_CONTENT)
FROM (
    SELECT METADATA$FILENAME, OBJECT_CONSTRUCT(*) 
    FROM @stg_finance_azure/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_csv_header')
PATTERN = '.*/customer.*\.csv';

-- PAYMENTS (Client C)
COPY INTO RAW.PAYMENTS_LANDING (SOURCE_PATH, RAW_CONTENT)
FROM (
    SELECT METADATA$FILENAME, OBJECT_CONSTRUCT(*) 
    FROM @stg_finance_azure/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_csv_header')
PATTERN = '.*/payments.*\.csv';

-- ORDERS (Client A & Client C)
COPY INTO RAW.ORDERS_LANDING (SOURCE_PATH, RAW_CONTENT)
FROM (
    SELECT METADATA$FILENAME, OBJECT_CONSTRUCT(*) 
    FROM @stg_finance_azure/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_csv_header')
PATTERN = '.*/order.*\.csv';

-- PRODUCTS (Client A & Client C)
COPY INTO RAW.PRODUCTS_LANDING (SOURCE_PATH, RAW_CONTENT)
FROM (
    SELECT METADATA$FILENAME, OBJECT_CONSTRUCT(*) 
    FROM @stg_finance_azure/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_csv_header')
PATTERN = '.*/product.*\.csv';