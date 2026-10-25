with fct as (
    select * from {{ ref('fct_demanda_logistica') }}
),

dim as (
    select * from {{ ref('dim_departamento') }}
)

select
    f.ranking_poblacion_nacional,
    f.cod_departamento,
    d.nombre_departamento,
    d.categoria,
    d.nombre_provincia,
    d.macro_region,
    d.latitud,
    d.longitud,
    f.poblacion,
    f.porcentaje_poblacion_nacional,
    f.porcentaje_poblacion_provincial,
    f.porcentaje_acumulado_pareto,
    f.penetracion_internet_hogares,
    f.indice_alcance_digital_hogares,
    f.ranking_alcance_digital,
    d.es_territorio_continental,
    d.requiere_logistica_especial
from fct f
inner join dim d
    on f.cod_departamento = d.cod_departamento
