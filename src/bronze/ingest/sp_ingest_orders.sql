CREATE OR REPLACE PROCEDURE RAW.SP_INGEST_ORDERS()
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
                'sku', $1, 
                'product_name', $2, 
                'category', $3, 
                'unit_price', $4,
                'currency', $5,
                'is_active', $6,
                'ingested_at', CURRENT_TIMESTAMP()
            )
        FROM @stg_finance_azure/
    )
    FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_CSV_HEADER')
    PATTERN = '(?i).*products?.*\.csv'
    ON_ERROR = 'ABORT_STATEMENT'
    ;

    RETURN 'Success: data cleaned and ingested';
END;
$$;