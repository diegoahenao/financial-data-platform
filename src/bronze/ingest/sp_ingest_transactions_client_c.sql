CREATE OR REPLACE PROCEDURE RAW.SP_INGEST_TRANSACTIONS_CLIENT_C()
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
            PARSE_JSON(
                REGEXP_REPLACE(
                    REGEXP_SUBSTR($1, '{.*}', 1, 1, 's'), 
                    '//.*', ''
                )
            )
        FROM @stg_finance_azure/client_c/
    )
    FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_XML_TXT')
    PATTERN = '(?i).*transactions?.*\\.(json)'
    ON_ERROR = 'ABORT_STATEMENT';

    RETURN 'Success: data ingested from JSON sources';
END;
$$;