{{ config(materialized='ephemeral') }}

WITH
src_data as (
    SELECT
           NAME             as EXCHANGE_NAME        -- TEXT
         , ID               as EXCHANGE_CODE        -- TEXT
         , COUNTRY          as EXCHANGE_COUNTRY     -- TEXT
         , CITY             as EXCHANGE_CITY        -- TEXT
         , ZONE             as TIMEZONE_CODE        -- TEXT
         , DELTA            as DELTA                -- TEXT
         , DST_PERIOD       as DST_PERIOD           -- TEXT
         , OPEN             as OPEN_TIME            -- TEXT
         , CLOSE            as CLOSE_TIME           -- TEXT
         , LUNCH            as LUNCH                -- TEXT
         , OPEN_UTC         as OPEN_UTC_TIME        -- TEXT
         , CLOSE_UTC        as CLOSE_UTC_TIME       -- TEXT
         , LUNCH_UTC        as LUNCH_UTC            -- TEXT
         , LOAD_TS          as LOAD_TS              -- TIMESTAMP_NTZ
         , 'SEED.EXCHANGE'  as RECORD_SOURCE

    FROM {{ source('seeds', 'EXCHANGE') }}
 ),

default_record as (
    SELECT
           'Missing'    as EXCHANGE_NAME
         , '-1'         as EXCHANGE_CODE
         , 'Missing'    as EXCHANGE_COUNTRY
         , 'Missing'    as EXCHANGE_CITY 
         , 'Missing'    as TIMEZONE_CODE
         , '-1'         as DELTA
         , 'Missing'    as DST_PERIOD
         , 'Missing'    as OPEN_TIME 
         , 'Missing'    as CLOSE_TIME
         , 'Missing'    as LUNCH
         , 'Missing'    as OPEN_UTC_TIME
         , 'Missing'    as CLOSE_UTC_TIME
         , 'Missing'    as LUNCH_UTC
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
        concat_ws('|', EXCHANGE_CODE) as EXCHANGE_HKEY
        , concat_ws('|', EXCHANGE_CODE, EXCHANGE_NAME, EXCHANGE_COUNTRY,
                         EXCHANGE_CITY, TIMEZONE_CODE, DELTA,
                         DST_PERIOD, OPEN_TIME, CLOSE_TIME,
                         LUNCH, OPEN_UTC_TIME, CLOSE_UTC_TIME,
                         LUNCH_UTC, LOAD_TS, RECORD_SOURCE) as EXCHANGE_HDIFF
        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed