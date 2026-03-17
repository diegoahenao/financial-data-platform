CREATE OR REPLACE PROCEDURE RAW.SP_INGEST_CUSTOMERS()
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
            OBJECT_CONSTRUCT('line_data', TRIM($1), 'ingested_at', CURRENT_TIMESTAMP())
        FROM @stg_finance_azure/
        (FILE_FORMAT => 'RAW.FF_TEXT')
        WHERE 
            $1 NOT LIKE '----- %' 
            AND $1 NOT LIKE 'customer_id%'
            AND TRIM($1) <> ''
    )
    PATTERN = '(?i).*customers?.*\.csv'
    ON_ERROR = 'CONTINUE';

    RETURN 'Success: data cleaned and ingested';
END;
$$;