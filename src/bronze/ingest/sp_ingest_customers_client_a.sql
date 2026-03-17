CREATE OR REPLACE PROCEDURE RAW.SP_INGEST_CUSTOMERS_CLIENT_A()
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
                'first_name', $2, 
                'last_name', $3, 
                'email', $4,
                'loyalty_tier', $5,
                'signup_source', $6,
                'is_active', $7,
                'ingested_at', CURRENT_TIMESTAMP()
            )
        FROM @stg_finance_azure/client_a/
    )
    FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
    PATTERN = '(?i).*customers?.*\.csv'
    ON_ERROR = 'ABORT_STATEMENT'
    ;

    RETURN 'Success: data cleaned and ingested';
END;
$$;