{{ config(materialized='ephemeral') }}

WITH
src_data as (
    SELECT
           ALPHABETIC_CODE  as ALPHABETIC_CODE        -- TEXT
         , NUMERIC_CODE     as NUMERIC_CODE           -- TEXT
         , DECIMAL_DIGITS   as DECIMAL_DIGITS         -- TEXT
         , CURRENCY_NAME    as CURRENCY_NAME          -- TEXT
         , LOCATIONS        as LOCATIONS              -- TEXT
         , LOAD_TS          as LOAD_TS                -- TIMESTAMP_NTZ
         , 'SEED.CURRENCY'  as RECORD_SOURCE

    FROM {{ source('seeds', 'CURRENCY') }}
),

default_record as (
    SELECT
           '-1'         as ALPHABETIC_CODE
         , '-1'         as NUMERIC_CODE
         , '0'          as DECIMAL_DIGITS
         , 'Missing'    as CURRENCY_NAME 
         , 'Missing'    as LOCATIONS
         , '2020-01-01' as LOAD_TS
         , 'Missing'    as RECORD_SOURCE
),

with_default_record as(
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed as (
    SELECT
        concat_ws('|', ALPHABETIC_CODE) as CURRENCY_HKEY
        , concat_ws('|', ALPHABETIC_CODE, NUMERIC_CODE, DECIMAL_DIGITS,
                         CURRENCY_NAME, LOCATIONS) as CURRENCY_HDIFF
        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed