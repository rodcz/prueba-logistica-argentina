-- Test de integridad: La suma de porcentajes de población nacional debe ser exactamente 100% (+/- 0.05% por redondeo)
with sum_pct as (
    select 
        sum(porcentaje_poblacion_nacional) as total_pct
    from {{ ref('fct_demanda_logistica') }}
)

select *
from sum_pct
where total_pct < 99.95 or total_pct > 100.05
