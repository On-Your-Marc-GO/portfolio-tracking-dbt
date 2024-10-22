{{ config(materialized='ephemeral') }}

WITH
src_data as (
    SELECT
           COUNTRY_NAME             as COUNTRY_NAME             -- TEXT
         , COUNTRY_CODE_2_LETTER    as COUNTRY_CODE_2_LETTER    -- TEXT
         , COUNTRY_CODE_3_LETTER    as COUNTRY_CODE_3_LETTER    -- TEXT
         , COUNTRY_CODE_NUMERIC     as COUNTRY_CODE_NUMERIC     -- TEXT
         , ISO_3166_2               as ISO_3166_2               -- TEXT
         , REGION                   as REGION                   -- TEXT
         , SUB_REGION               as SUB_REGION               -- TEXT
         , INTERMEDIATE_REGION      as INTERMEDIATE_REGION      -- TEXT
         , REGION_CODE              as REGION_CODE              -- TEXT
         , SUB_REGION_CODE          as SUB_REGION_CODE          -- TEXT
         , INTERMEDIATE_REGION_CODE as INTERMEDIATE_REGION_CODE -- TEXT
         , LOAD_TS                  as LOAD_TS                  -- TIMESTAMP_NTZ
         , 'SEED.COUNTRY'           as RECORD_SOURCE            -- TEXT

    FROM {{ source('seeds', 'COUNTRY') }}
),

default_record as (
    SELECT
           'Missing'        as COUNTRY_NAME
         , '-1'             as COUNTRY_CODE_2_LETTER
         , '-1'             as COUNTRY_CODE_3_LETTER
         , '-1'             as COUNTRY_CODE_NUMERIC 
         , 'Missing'        as ISO_3166_2
         , 'Missing'        as REGION
         , 'Missing'        as SUB_REGION
         , 'Missing'        as INTERMEDIATE_REGION
         , '-1'             as REGION_CODE
         , '-1'             as SUB_REGION_CODE
         , '-1'             as INTERMEDIATE_REGION_CODE
         , '2020-01-01'     as LOAD_TS
         , 'Missing'        as RECORD_SOURCE
),

with_default_record as(
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed as (
    SELECT
        concat_ws('|', COUNTRY_CODE_2_LETTER, COUNTRY_CODE_3_LETTER, COUNTRY_CODE_NUMERIC) as COUNTRY_HKEY
        , concat_ws('|', COUNTRY_NAME, COUNTRY_CODE_2_LETTER, COUNTRY_CODE_3_LETTER,
                         COUNTRY_CODE_NUMERIC, ISO_3166_2, REGION,
                         SUB_REGION, INTERMEDIATE_REGION, REGION_CODE,
                         SUB_REGION_CODE, INTERMEDIATE_REGION_CODE) as COUNTRY_HDIFF
        , * EXCLUDE LOAD_TS
        , LOAD_TS as LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed