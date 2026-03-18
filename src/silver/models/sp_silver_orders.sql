CREATE OR REPLACE PROCEDURE SILVER.SP_SILVER_ORDERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO SILVER.ORDERS target
    USING (
        SELECT 
            RAW_CONTENT:order_id::VARCHAR as ORDER_ID,
            RAW_CONTENT:customer_id::VARCHAR as CUSTOMER_ID,
            RAW_CONTENT:order_date::VARCHAR as ORDER_DATE,
            TRIM(SPLIT_PART(RAW_CONTENT:order_status, '<', 1))::VARCHAR as ORDER_STATUS,
            TRIM(SPLIT_PART(RAW_CONTENT:channel, '<', 1))::VARCHAR as CHANNEL,
            UPPER(SPLIT_PART(SOURCE_PATH, '/', 1)) as SOURCE_SYSTEM,
            HASH(RAW_CONTENT:order_id, RAW_CONTENT:customer_id, RAW_CONTENT:order_date, RAW_CONTENT:order_status, RAW_CONTENT:channel) as TRACKINGHASH,
            RAW_CONTENT:ingested_at::VARCHAR as INGESTED_AT
        FROM RAW.ORDERS_LANDING 
        WHERE SOURCE_PATH LIKE '%client_a%'
        AND RAW_CONTENT:order_id NOT LIKE '-----%'

        UNION ALL

        SELECT 
            RAW_CONTENT:order_id::VARCHAR as ORDER_ID,
            RAW_CONTENT:customer_id::VARCHAR as CUSTOMER_ID,
            RAW_CONTENT:order_date::VARCHAR as ORDER_DATE,
            TRIM(SPLIT_PART(RAW_CONTENT:order_status, '<', 1))::VARCHAR as ORDER_STATUS,
            NULL as CHANNEL,
            UPPER(SPLIT_PART(SOURCE_PATH, '/', 1)) as SOURCE_SYSTEM,
            HASH(RAW_CONTENT:order_id, RAW_CONTENT:customer_id, RAW_CONTENT:order_date, RAW_CONTENT:order_status) as TRACKINGHASH,
            RAW_CONTENT:ingested_at::VARCHAR as INGESTED_AT
        FROM RAW.ORDERS_LANDING 
        WHERE SOURCE_PATH LIKE '%client_c%'
        AND RAW_CONTENT:customer_id NOT LIKE '-----%'
    ) source
    ON target.ORDER_ID = source.ORDER_ID 
       AND target.SOURCE_SYSTEM = source.SOURCE_SYSTEM

    WHEN MATCHED AND target.TRACKINGHASH <> source.TRACKINGHASH THEN 
        UPDATE SET 
            target.CUSTOMER_ID = source.CUSTOMER_ID,
            target.ORDER_DATE = source.ORDER_DATE,
            target.ORDER_STATUS = source.ORDER_STATUS,
            target.TRACKINGHASH = source.TRACKINGHASH,
            target.INGESTED_AT = source.INGESTED_AT
            
    WHEN NOT MATCHED THEN 
        INSERT (ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS, SOURCE_SYSTEM, TRACKINGHASH, INGESTED_AT)
        VALUES (source.ORDER_ID, source.CUSTOMER_ID, source.ORDER_DATE, source.ORDER_STATUS, source.SOURCE_SYSTEM, source.TRACKINGHASH, source.INGESTED_AT);

    RETURN 'Success: Orders Silver Idempotent Update';
END;
$$;