CREATE OR REPLACE PROCEDURE RAW.SP_INGEST_TRANSACTIONS_CLIENT_A()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    COPY INTO RAW.TRANSACTIONS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (
        SELECT 
            METADATA$FILENAME, 
            $1::VARIANT
        FROM @stg_finance_azure/client_a/
    )
    FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_XML_TXT')
    PATTERN = '(?i).*transactions?.*\\.(xml|txt)'
    ON_ERROR = 'ABORT_STATEMENT'
    ;

    RETURN 'Success: data ingested from XML and TXT sources';
END;
$$;