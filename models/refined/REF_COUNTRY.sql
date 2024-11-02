WITH

current_from_snapshot as (
    {{ current_from_snapshot(
        snsh_ref = ref('SNSH_COUNTRY') 
    ) }}
)


SELECT *
FROM current_from_snapshot