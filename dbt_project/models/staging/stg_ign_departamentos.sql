with source as (
    select * from {{ source('raw', 'ign_departamentos') }}
),

renamed as (
    select
        lpad(id, 5, '0') as cod_departamento,
        lpad(provincia_id, 2, '0') as cod_provincia,
        upper(trim(nombre)) as nombre_departamento,
        upper(trim(provincia_nombre)) as nombre_provincia,
        categoria,
        cast(centroide_lat as double precision) as centroide_lat,
        cast(centroide_lon as double precision) as centroide_lon,
        fuente
    from source
)

select * from renamed
