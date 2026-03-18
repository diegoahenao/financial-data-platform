CREATE OR REPLACE PROCEDURE SILVER.SP_SILVER_PRODUCTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO SILVER.PRODUCTS target
    USING (
        SELECT 
            RAW_CONTENT:sku::VARCHAR as PRODUCT_ID,
            RAW_CONTENT:product_name::VARCHAR as PRODUCT_NAME,
            RAW_CONTENT:category::VARCHAR as CATEGORY,
            RAW_CONTENT:unit_price::VARCHAR as UNIT_PRICE,
            RAW_CONTENT:currency::VARCHAR as CURRENCY,
            TRY_TO_BOOLEAN(TRIM(SPLIT_PART(RAW_CONTENT:is_active, '<', 1)))::BOOLEAN as IS_ACTIVE,
            UPPER(SPLIT_PART(SOURCE_PATH, '/', 1)) as SOURCE_SYSTEM,
            HASH(RAW_CONTENT:sku, RAW_CONTENT:product_name, RAW_CONTENT:category, RAW_CONTENT:unit_price, RAW_CONTENT:currency, RAW_CONTENT:is_active) as TRACKINGHASH,
            RAW_CONTENT:ingested_at::VARCHAR as INGESTED_AT
        FROM RAW.PRODUCTS_LANDING 
        WHERE SOURCE_PATH LIKE '%client_a%'
        AND RAW_CONTENT:sku NOT LIKE '-----%'

        UNION ALL

        SELECT 
            RAW_CONTENT:sku::VARCHAR as PRODUCT_ID,
            RAW_CONTENT:product_name::VARCHAR as PRODUCT_NAME,
            RAW_CONTENT:category::VARCHAR as CATEGORY,
            RAW_CONTENT:unit_price::VARCHAR as UNIT_PRICE,
            RAW_CONTENT:currency::VARCHAR as CURRENCY,
            TRY_TO_BOOLEAN(TRIM(SPLIT_PART(RAW_CONTENT:is_active, '<', 1)))::BOOLEAN as IS_ACTIVE,
            UPPER(SPLIT_PART(SOURCE_PATH, '/', 1)) as SOURCE_SYSTEM,
            HASH(RAW_CONTENT:sku, RAW_CONTENT:product_name, RAW_CONTENT:category, RAW_CONTENT:unit_price, RAW_CONTENT:currency, RAW_CONTENT:is_active) as TRACKINGHASH,
            RAW_CONTENT:ingested_at::VARCHAR as INGESTED_AT
        FROM RAW.PRODUCTS_LANDING 
        WHERE SOURCE_PATH LIKE '%client_c%'
        AND RAW_CONTENT:customer_id NOT LIKE '-----%'
    ) source
    ON target.PRODUCT_ID = source.PRODUCT_ID 
       AND target.SOURCE_SYSTEM = source.SOURCE_SYSTEM
       
    WHEN MATCHED AND target.TRACKINGHASH <> source.TRACKINGHASH THEN 
        UPDATE SET 
            target.PRODUCT_ID = source.PRODUCT_ID,
            target.PRODUCT_NAME = source.PRODUCT_NAME,
            target.CATEGORY = source.CATEGORY,
            target.UNIT_PRICE = source.UNIT_PRICE,
            target.CURRENCY = source.CURRENCY,
            target.IS_ACTIVE = source.IS_ACTIVE,           
            target.TRACKINGHASH = source.TRACKINGHASH,
            target.INGESTED_AT = source.INGESTED_AT
            
    WHEN NOT MATCHED THEN 
        INSERT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, CURRENCY, IS_ACTIVE, SOURCE_SYSTEM, TRACKINGHASH, INGESTED_AT)
        VALUES (source.PRODUCT_ID, source.PRODUCT_NAME, source.CATEGORY, source.UNIT_PRICE, source.CURRENCY, source.IS_ACTIVE, source.SOURCE_SYSTEM, source.TRACKINGHASH, source.INGESTED_AT);

    RETURN 'Success: Products Silver Idempotent Update';
END;
$$;