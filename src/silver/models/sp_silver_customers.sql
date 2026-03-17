CREATE OR REPLACE PROCEDURE SILVER.SP_SILVER_CUSTOMERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO SILVER.CUSTOMERS target
    USING (
        SELECT 
            RAW_CONTENT:customer_id::VARCHAR as CUSTOMER_ID,
            RAW_CONTENT:first_name::VARCHAR as FIRST_NAME,
            RAW_CONTENT:last_name::VARCHAR as LAST_NAME,
            RAW_CONTENT:email::VARCHAR as EMAIL,
            RAW_CONTENT:loyalty_tier::VARCHAR as LOYALTY_TIER,
            TRIM(SPLIT_PART(RAW_CONTENT:signup_source, '<', 1))::VARCHAR as SIGNUP_SOURCE,
            RAW_CONTENT:segment::VARCHAR as SEGMENT,
            TRY_TO_BOOLEAN(TRIM(SPLIT_PART(RAW_CONTENT:is_active, '<', 1)))::BOOLEAN as IS_ACTIVE,
            UPPER(SPLIT_PART(SOURCE_PATH, '/', 1)) as SOURCE_SYSTEM,
            HASH(RAW_CONTENT:customer_id, RAW_CONTENT:first_name, RAW_CONTENT:last_name, RAW_CONTENT:email, RAW_CONTENT:loyalty_tier, RAW_CONTENT:signup_source, RAW_CONTENT:segment, RAW_CONTENT:is_active) as TRACKINGHASH,
            RAW_CONTENT:ingested_at::VARCHAR as INGESTED_AT
        FROM RAW.CUSTOMERS_LANDING 
        WHERE SOURCE_PATH LIKE '%client_a%'
        AND RAW_CONTENT:customer_id NOT LIKE '-----%'

        UNION ALL

        SELECT 
            RAW_CONTENT:customer_id::VARCHAR as CUSTOMER_ID,
            SPLIT_PART(RAW_CONTENT:customer_name, ' ', 1)::VARCHAR as FIRST_NAME,
            SPLIT_PART(RAW_CONTENT:customer_name, ' ', 2)::VARCHAR as LAST_NAME,
            RAW_CONTENT:email::VARCHAR as EMAIL,
            NULL as LOYALTY_TIER,
            NULL as SIGNUP_SOURCE,
            RAW_CONTENT:segment::VARCHAR as SEGMENT,
            TRY_TO_BOOLEAN(TRIM(SPLIT_PART(RAW_CONTENT:is_active, '<', 1)))::BOOLEAN as IS_ACTIVE,
            UPPER(SPLIT_PART(SOURCE_PATH, '/', 1)) as SOURCE_SYSTEM,
            HASH(RAW_CONTENT:customer_id, RAW_CONTENT:customer_name, RAW_CONTENT:segment, RAW_CONTENT:is_active) as TRACKINGHASH,
            RAW_CONTENT:ingested_at::VARCHAR as INGESTED_AT
        FROM RAW.CUSTOMERS_LANDING 
        WHERE SOURCE_PATH LIKE '%client_c%'
        AND RAW_CONTENT:customer_id NOT LIKE '-----%'
    ) source
    ON target.CUSTOMER_ID = source.CUSTOMER_ID 
       AND target.SOURCE_SYSTEM = source.SOURCE_SYSTEM
       AND target.TRACKINGHASH <> source.TRACKINGHASH
    WHEN MATCHED THEN 
        UPDATE SET 
            target.FIRST_NAME = source.FIRST_NAME,
            target.LAST_NAME = source.LAST_NAME,
            target.EMAIL = source.EMAIL,
            target.LOYALTY_TIER = source.LOYALTY_TIER,
            target.SIGNUP_SOURCE = source.SIGNUP_SOURCE,
            target.SEGMENT = source.SEGMENT,
            target.IS_ACTIVE = source.IS_ACTIVE,
            target.TRACKINGHASH = source.TRACKINGHASH,
            target.INGESTED_AT = source.INGESTED_AT
            
    WHEN NOT MATCHED THEN 
        INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, LOYALTY_TIER, SIGNUP_SOURCE, SEGMENT, IS_ACTIVE, SOURCE_SYSTEM, TRACKINGHASH, INGESTED_AT)
        VALUES (source.CUSTOMER_ID, source.FIRST_NAME, source.LAST_NAME, source.EMAIL, source.LOYALTY_TIER, source.SIGNUP_SOURCE, source.SEGMENT, source.IS_ACTIVE, source.SOURCE_SYSTEM, source.TRACKINGHASH, source.INGESTED_AT);

    RETURN 'Success: Customers Silver Idempotent Update';
END;
$$;