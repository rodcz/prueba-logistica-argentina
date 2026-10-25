with source as (
    select * from {{ source('raw', 'enacom_conectividad') }}
),

latest_period as (
    select 
        max(anio) as max_anio, 
        max(trimestre) as max_trimestre
    from source
    where anio = (select max(anio) from source)
),

filtered as (
    select
        s.anio,
        s.trimestre,
        upper(trim(s.provincia)) as provincia_enacom,
        cast(s.accesos_cada_100_hogares as numeric(10,2)) as penetracion_hogares_pct,
        cast(s.accesos_cada_100_habitantes as numeric(10,2)) as penetracion_habitantes_pct
    from source s
    inner join latest_period lp
        on s.anio = lp.max_anio and s.trimestre = lp.max_trimestre
),

standardized as (
    select
        anio,
        trimestre,
        provincia_enacom,
        case
            when provincia_enacom like '%BUENOS AIRES%' and provincia_enacom not like '%CABA%' and provincia_enacom not like '%CIUDAD%' then '06'
            when provincia_enacom like '%CABA%' or provincia_enacom like '%CAPITAL%' or provincia_enacom like '%CIUDAD AUT%' then '02'
            when provincia_enacom like '%CATAMARCA%' then '10'
            when provincia_enacom like '%CORDOBA%' or provincia_enacom like '%CÓRDOBA%' then '14'
            when provincia_enacom like '%CORRIENTES%' then '18'
            when provincia_enacom like '%CHACO%' then '22'
            when provincia_enacom like '%CHUBUT%' then '26'
            when provincia_enacom like '%ENTRE RIOS%' or provincia_enacom like '%ENTRE RÍOS%' then '30'
            when provincia_enacom like '%FORMOSA%' then '34'
            when provincia_enacom like '%JUJUY%' then '38'
            when provincia_enacom like '%LA PAMPA%' then '42'
            when provincia_enacom like '%LA RIOJA%' then '46'
            when provincia_enacom like '%MENDOZA%' then '50'
            when provincia_enacom like '%MISIONES%' then '54'
            when provincia_enacom like '%NEUQUEN%' or provincia_enacom like '%NEUQUÉN%' then '58'
            when provincia_enacom like '%RIO NEGRO%' or provincia_enacom like '%RÍO NEGRO%' then '62'
            when provincia_enacom like '%SALTA%' then '66'
            when provincia_enacom like '%SAN JUAN%' then '70'
            when provincia_enacom like '%SAN LUIS%' then '74'
            when provincia_enacom like '%SANTA CRUZ%' then '78'
            when provincia_enacom like '%SANTA FE%' then '82'
            when provincia_enacom like '%SANTIAGO DEL ESTERO%' then '86'
            when provincia_enacom like '%TUCUMAN%' or provincia_enacom like '%TUCUMÁN%' then '90'
            when provincia_enacom like '%TIERRA DEL FUEGO%' then '94'
            else null
        end as cod_provincia,
        penetracion_hogares_pct,
        penetracion_habitantes_pct
    from filtered
)

select * from standardized
