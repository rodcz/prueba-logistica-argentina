with source as (
    select * from {{ source('raw', 'indec_poblacion_departamentos') }}
),

renamed as (
    select
        lpad(departamento_id, 5, '0') as cod_departamento,
        lpad(provincia_id, 2, '0') as cod_provincia,
        upper(trim(nombre_departamento)) as nombre_departamento,
        upper(trim(nombre_provincia)) as nombre_provincia,
        cast(poblacion_total as bigint) as poblacion_total
    from source
)

select * from renamed
