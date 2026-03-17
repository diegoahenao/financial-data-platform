CREATE OR REPLACE PROCEDURE RAW.SP_INGEST_CUSTOMERS_CLIENT_C()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    COPY INTO RAW.CUSTOMERS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (
        SELECT 
            METADATA$FILENAME, 
            OBJECT_CONSTRUCT(
                'customer_id', $1, 
                'customer_name', $2, 
                'email', $3, 
                'segment', $4,
                'is_active', $5,
                'ingested_at', CURRENT_TIMESTAMP()
            )
        FROM @stg_finance_azure/client_c/
    )
    FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
    PATTERN = '(?i).*customers?.*\.csv'
    ON_ERROR = 'ABORT_STATEMENT'
    ;

    RETURN 'Success: data cleaned and ingested';
END;
$$;