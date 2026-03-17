CREATE OR REPLACE PROCEDURE RAW.SP_INGEST_ORDERS_CLIENT_C()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    COPY INTO RAW.ORDERS_LANDING (SOURCE_PATH, RAW_CONTENT)
    FROM (
        SELECT 
            METADATA$FILENAME, 
            OBJECT_CONSTRUCT(
                'order_id', $1, 
                'customer_id', $2, 
                'order_date', $3, 
                'order_status', $4,
                'ingested_at', CURRENT_TIMESTAMP()
            )
        FROM @stg_finance_azure/client_c/
    )
    FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
    PATTERN = '(?i).*orders?.*\.csv'
    ON_ERROR = 'ABORT_STATEMENT'
    ;

    RETURN 'Success: data cleaned and ingested';
END;
$$;