with curated as (
    select * from {{ ref('int_departamentos_curated') }}
)

select
    cod_departamento,
    nombre_departamento,
    categoria,
    cod_provincia,
    nombre_provincia,
    macro_region,
    es_amba,
    centroide_lat as latitud,
    centroide_lon as longitud,
    es_territorio_continental,
    requiere_logistica_especial
from curated
