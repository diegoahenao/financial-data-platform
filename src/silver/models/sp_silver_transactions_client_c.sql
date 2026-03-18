CREATE OR REPLACE PROCEDURE SILVER.SP_SILVER_TRANSACTIONS_CLIENT_A()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO SILVER.TRANSACTIONS target
    USING (
        WITH base_flatten AS (
            SELECT 
                SOURCE_PATH,
                LOAD_TIMESTAMP,
                t.value as txn_json
            FROM RAW.TRANSACTIONS_LANDING,
            LATERAL FLATTEN(input => RAW_CONTENT:transactions) as t
            WHERE SOURCE_PATH LIKE '%client_c%'
        ),
        extracted_data AS (
            SELECT
                txn_json:id::VARCHAR as TXN_ID,
                txn_json:order.id::VARCHAR as ORDER_ID,
                txn_json:order.date::DATE as ORDER_DATE,
                txn_json:order.customer.id::VARCHAR as CUSTOMER_ID,
                SPLIT_PART(txn_json:order.customer.name::VARCHAR, ' ', 1) as FIRST_NAME,
                REPLACE(txn_json:order.customer.name::VARCHAR, SPLIT_PART(txn_json:order.customer.name::VARCHAR, ' ', 1) || ' ', '') as LAST_NAME,
                txn_json:order.customer.email::VARCHAR as EMAIL,
                NULL as NOTE,
                it.value:sku::VARCHAR as PRODUCT_ID,
                it.value:description::VARCHAR as PRODUCT_DESCRIPTION,
                it.value:qty::NUMBER(10,2) as QUANTITY,
                it.value:price.amount::NUMBER(15,2) as UNIT_PRICE,
                it.value:price.currency::VARCHAR as UNIT_PRICE_CURRENCY,
                txn_json:payment.method::VARCHAR as PAYMENT_METHOD,
                txn_json:payment.total::NUMBER(15,2) as AMOUNT,
                it.value:price.currency::VARCHAR as AMOUNT_CURRENCY,                
                UPPER(SPLIT_PART(SOURCE_PATH, '/', 1)) as SOURCE_SYSTEM,
                LOAD_TIMESTAMP as INGESTED_AT
            FROM base_flatten,
            LATERAL FLATTEN(input => txn_json:items, OUTER => TRUE) as it
        ) 
        SELECT 
            TXN_ID,
            ORDER_ID,
            ORDER_DATE,
            CUSTOMER_ID
            FIRST_NAME,
            LAST_NAME,
            EMAIL,
            NOTE,
            PRODUCT_ID,
            PRODUCT_DESCRIPTION,
            QUANTITY,
            UNIT_PRICE,
            UNIT_PRICE_CURRENCY,
            PAYMENT_MEHTOD,
            AMOUNT,
            AMOUNT_CURRENCY,
            SOURCE_SYSTEM,
            HASH(ORDER_ID, ORDER_DATE, CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, NOTE, PRODUCT_ID, PRODUCT_DESCRIPTION, QUANTITY, UNIT_PRICE, UNIT_PRICE_CURRENCY, PAYMENT_MEHTOD, AMOUNT, AMOUNT_CURRENCY) as TRACKINGHASH,
            INGESTED_AT
        FROM extracted_data
    ) source
    ON target.TXN_ID = source.TXN_ID 
       AND target.PRODUCT_ID = source.PRODUCT_ID
       AND target.SOURCE_SYSTEM = source.SOURCE_SYSTEM
       AND target.TRACKINGHASH <> source.TRACKINGHASH

    WHEN MATCHED THEN
        UPDATE SET 
            target.ORDER_ID = source.ORDER_ID,
            target.ORDER_DATE = source.ORDER_DATE,
            target.CUSTOMER_ID = source.CUSTOMER_ID,
            target.FIRST_NAME = source.FIRST_NAME,
            target.LAST_NAME = source.LAST_NAME,
            target.EMAIL = source.EMAIL,
            target.NOTE = source.NOTE,
            target.PRODUCT_ID = source.PRODUCT_ID,
            target.PRODUCT_DESCRIPTION = source.PRODUCT_DESCRIPTION,
            target.QUANTITY = source.QUANTITY,
            target.UNIT_PRICE = source.UNIT_PRICE,
            target.UNIT_PRICE_CURRENCY = source.UNIT_PRICE_CURRENCY,
            target.PAYMENT_METHOD = source.PAYMENT_METHOD,
            target.AMOUNT = source.AMOUNT,
            target.AMOUNT_CURRENCY = source.AMOUNT_CURRENCY,
            target.SOURCE_SYSTEM = source.SOURCE_SYSTEM,
            target.TRACKINGHASH = source.TRACKINGHASH,
            target.INGESTED_AT = source.INGESTED_AT

    WHEN NOT MATCHED THEN
        INSERT (
            TXN_ID, ORDER_ID, ORDER_DATE, CUSTOMER_ID, 
            FIRST_NAME, LAST_NAME, EMAIL, NOTE, PRODUCT_ID, 
            PRODUCT_DESCRIPTION, QUANTITY, UNIT_PRICE, UNIT_PRICE_CURRENCY, 
            PAYMENT_METHOD, AMOUNT, AMOUNT_CURRENCY, SOURCE_SYSTEM, 
            TRACKINGHASH, INGESTED_AT
        )
        VALUES (
            source.TXN_ID, source.ORDER_ID, source.ORDER_DATE, source.CUSTOMER_ID, 
            source.FIRST_NAME, source.LAST_NAME, source.EMAIL, source.NOTE, source.PRODUCT_ID, 
            source.PRODUCT_DESCRIPTION, source.QUANTITY, source.UNIT_PRICE, source.UNIT_PRICE_CURRENCY, 
            source.PAYMENT_METHOD, source.AMOUNT, source.AMOUNT_CURRENCY, source.SOURCE_SYSTEM, 
            source.TRACKINGHASH, source.INGESTED_AT
        );


        RETURN 'Success: JSON Transactions processed into Silver. Idempotency guaranteed.';
    END;
    $$;