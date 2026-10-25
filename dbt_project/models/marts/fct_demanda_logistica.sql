with curated as (
    select * from {{ ref('int_departamentos_curated') }}
),

totals as (
    select
        sum(poblacion) as poblacion_nacional_total
    from curated
),

provincial_totals as (
    select
        cod_provincia,
        sum(poblacion) as poblacion_provincial_total
    from curated
    group by cod_provincia
),

calculated as (
    select
        c.cod_departamento,
        c.cod_provincia,
        c.nombre_departamento,
        c.nombre_provincia,
        c.macro_region,
        c.poblacion,
        
        -- Participación respecto al total nacional (Pregunta 3)
        round((c.poblacion * 100.0 / t.poblacion_nacional_total), 4) as porcentaje_poblacion_nacional,
        
        -- Participación respecto a la provincia (Pregunta 2)
        round((c.poblacion * 100.0 / pt.poblacion_provincial_total), 4) as porcentaje_poblacion_provincial,
        
        -- Ranking nacional de población (Pregunta 1)
        dense_rank() over (order by c.poblacion desc) as ranking_poblacion_nacional,
        
        -- Ranking provincial de población
        dense_rank() over (partition by c.cod_provincia order by c.poblacion desc) as ranking_poblacion_provincial,
        
        -- Cuota acumulada Pareto (Pregunta 3)
        round(
            sum(c.poblacion * 100.0 / t.poblacion_nacional_total) over (
                order by c.poblacion desc 
                rows between unbounded preceding and current row
            ), 4
        ) as porcentaje_acumulado_pareto,
        
        -- Métrica diferencial logística digital (Pregunta 4: IADH)
        c.penetracion_internet_hogares,
        round(c.poblacion * (c.penetracion_internet_hogares / 100.0)) as indice_alcance_digital_hogares,
        dense_rank() over (order by (c.poblacion * (c.penetracion_internet_hogares / 100.0)) desc) as ranking_alcance_digital

    from curated c
    cross join totals t
    inner join provincial_totals pt
        on c.cod_provincia = pt.cod_provincia
)

select * from calculated
