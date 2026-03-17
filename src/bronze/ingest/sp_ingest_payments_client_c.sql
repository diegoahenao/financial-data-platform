CREATE OR REPLACE PROCEDURE RAW.SP_INGEST_PAYMENTS_CLIENT_C()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    COPY INTO RAW.PAYMENTS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (
        SELECT 
            METADATA$FILENAME, 
            OBJECT_CONSTRUCT(
                'payment_id', $1, 
                'order_id', $2, 
                'payment_method', $3, 
                'amount', $4,
                'currency', $5,
                'status', $6,
                'ingested_at', CURRENT_TIMESTAMP()
            )
        FROM @stg_finance_azure/client_c/
    )
    FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
    PATTERN = '(?i).*payments?.*\.csv'
    ON_ERROR = 'ABORT_STATEMENT'
    ;

    RETURN 'Success: data cleaned and ingested';
END;
$$;