CREATE OR REPLACE PROCEDURE SILVER.SP_SILVER_TRANSACTIONS_CLIENT_A()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO SILVER.TRANSACTIONS target
    USING (
        WITH raw_cleaned AS (
            SELECT 
                SOURCE_PATH,
                LOAD_TIMESTAMP,
                REGEXP_SUBSTR(RAW_CONTENT, '<Transaction>.*</Transaction>', 1, 1, 's') as CLEAN_PAYLOAD
            FROM RAW.TRANSACTIONS_LANDING
            WHERE RAW_CONTENT IS NOT NULL
        ),
        
        xml_table AS (SELECT 
            XMLGET(t.value, 'TransactionID'):"$" ::VARCHAR as TXN_ID,
            XMLGET(XMLGET(t.value, 'Order'), 'OrderID'):"$" ::VARCHAR as ORDER_ID,
            XMLGET(XMLGET(t.value, 'Order'), 'OrderDate'):"$" ::VARCHAR as ORDER_DATE,
            XMLGET(XMLGET(XMLGET(t.value, 'Order'), 'Customer'), 'CustomerID'):"$" ::VARCHAR as CUSTOMER_ID,
            XMLGET(XMLGET(XMLGET(XMLGET(t.value, 'Order'), 'Customer'), 'Name'), 'FirstName'):"$" ::VARCHAR as FIRST_NAME,
            XMLGET(XMLGET(XMLGET(XMLGET(t.value, 'Order'), 'Customer'), 'Name'), 'LastName'):"$" ::VARCHAR as LAST_NAME,
            XMLGET(XMLGET(XMLGET(t.value, 'Order'), 'Customer'), 'Email'):"$" ::VARCHAR as EMAIL,
            XMLGET(XMLGET(XMLGET(XMLGET(t.value, 'Order'), 'Customer'), 'Notes'), 'Note'):"$" ::VARCHAR as NOTE,
            XMLGET(XMLGET(XMLGET(t.value, 'Items'), 'Item'), 'SKU'):"$" ::VARCHAR as PRODUCT_ID,
            XMLGET(XMLGET(XMLGET(t.value, 'Items'), 'Item'), 'Description'):"$" ::VARCHAR as PRODUCT_DESCRIPTION,
            XMLGET(XMLGET(XMLGET(t.value, 'Items'), 'Item'), 'Quantity'):"$" ::VARCHAR as QUANTITY,
            XMLGET(XMLGET(XMLGET(t.value, 'Items'), 'Item'), 'UnitPrice'):"$" ::VARCHAR as UNIT_PRICE,
            XMLGET(XMLGET(XMLGET(t.value, 'Items'), 'Item'), 'UnitPrice'):"@currency" ::VARCHAR as UNIT_PRICE_CURRENCY,
            XMLGET(XMLGET(t.value, 'Payment'), 'Method'):"$" ::VARCHAR as PAYMENT_METHOD,
            XMLGET(XMLGET(t.value, 'Payment'), 'Amount'):"$" ::VARCHAR as AMOUNT,
            XMLGET(XMLGET(t.value, 'Payment'), 'Amount'):"@currency" ::VARCHAR as AMOUNT_CURRENCY,
            UPPER(SPLIT_PART(SOURCE_PATH, '/', 1)) as SOURCE_SYSTEM,
            LOAD_TIMESTAMP
        FROM raw_cleaned,
        LATERAL FLATTEN(input => PARSE_XML('<root>' || CLEAN_PAYLOAD || '</root>'):"$") as t)
        SELECT 
            TXN_ID,
            ORDER_ID,
            ORDER_DATE,
            CUSTOMER_ID,
            FIRST_NAME,
            LAST_NAME,
            EMAIL,
            NOTE,
            PRODUCT_ID,
            PRODUCT_DESCRIPTION,
            QUANTITY,
            UNIT_PRICE,
            UNIT_PRICE_CURRENCY,
            PAYMENT_METHOD,
            AMOUNT,
            AMOUNT_CURRENCY,
            SOURCE_SYSTEM,
            HASH(TXN_ID, ORDER_ID, ORDER_DATE, CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, NOTE, PRODUCT_ID, PRODUCT_DESCRIPTION, QUANTITY, UNIT_PRICE, UNIT_PRICE_CURRENCY, PAYMENT_METHOD, PAYMENT_METHOD, AMOUNT, AMOUNT_CURRENCY) as TRACKINGHASH,
            LOAD_TIMESTAMP AS INGESTED_AT
        FROM xml_table
        WHERE TXN_ID IS NOT NULL
    ) source
    ON target.TXN_ID = source.TXN_ID
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
            target.TRACKINGHASH = source.TRACKINGHASH,
            target.INGESTED_AT = source.INGESTED_AT
    WHEN NOT MATCHED THEN 
        INSERT (TXN_ID, ORDER_ID, ORDER_DATE, CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, NOTE, PRODUCT_ID, PRODUCT_DESCRIPTION, QUANTITY, UNIT_PRICE, UNIT_PRICE_CURRENCY, PAYMENT_METHOD, AMOUNT, AMOUNT_CURRENCY, SOURCE_SYSTEM, TRACKINGHASH, INGESTED_AT)
        VALUES (source.TXN_ID, source.ORDER_ID, source.ORDER_DATE, source.CUSTOMER_ID, source.FIRST_NAME, source.LAST_NAME, source.EMAIL, source.NOTE, source.PRODUCT_ID, source.PRODUCT_DESCRIPTION, source.QUANTITY, source.UNIT_PRICE, source.UNIT_PRICE_CURRENCY, source.PAYMENT_METHOD, source.AMOUNT, source.AMOUNT_CURRENCY, source.SOURCE_SYSTEM, source.TRACKINGHASH, source.INGESTED_AT);

        RETURN 'Success: XML Transactions processed into Silver. Idempotency guaranteed.';
    END;
    $$;