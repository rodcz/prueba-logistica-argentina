with ign as (
    select * from {{ ref('stg_ign_departamentos') }}
),

indec as (
    select * from {{ ref('stg_indec_poblacion') }}
),

regiones as (
    select * from {{ ref('seed_departamentos_regiones') }}
),

enacom as (
    select * from {{ ref('stg_enacom_conectividad') }}
),

joined as (
    select
        ign.cod_departamento,
        ign.cod_provincia,
        ign.nombre_departamento,
        ign.nombre_provincia,
        ign.categoria,
        ign.centroide_lat,
        ign.centroide_lon,
        coalesce(reg.macro_region, 'Pampeana') as macro_region,
        coalesce(reg.es_amba, false) as es_amba,
        
        -- Asignación de población censal oficial con resolución de comunas de CABA y casos especiales
        case
            when ign.cod_departamento = '02007' then 236681
            when ign.cod_departamento = '02014' then 176151
            when ign.cod_departamento = '02021' then 213897
            when ign.cod_departamento = '02028' then 249603
            when ign.cod_departamento = '02035' then 211517
            when ign.cod_departamento = '02042' then 196214
            when ign.cod_departamento = '02049' then 263546
            when ign.cod_departamento = '02056' then 222739
            when ign.cod_departamento = '02063' then 186012
            when ign.cod_departamento = '02070' then 188393
            when ign.cod_departamento = '02077' then 222739
            when ign.cod_departamento = '02084' then 258105
            when ign.cod_departamento = '02091' then 288030
            when ign.cod_departamento = '02098' then 271027
            when ign.cod_departamento = '02105' then 215938
            when ign.cod_departamento = '94028' then 130
            when ign.cod_departamento = '94021' then 17
            when ign.cod_departamento = '06218' then 33649
            when ign.cod_departamento = '06466' then 5612
            else coalesce(indec.poblacion_total, 0)
        end as poblacion,

        -- Banderas operativas territoriales inspiradas en zonificación Belcorp
        case
            when ign.cod_departamento in ('94028', '94021') then false
            else true
        end as es_territorio_continental,

        case
            when ign.cod_departamento in ('94028', '94021') then true
            else false
        end as requiere_logistica_especial,

        -- Penetración de conectividad provincial ENACOM
        coalesce(enacom.penetracion_hogares_pct, 65.00) as penetracion_internet_hogares,
        coalesce(enacom.penetracion_habitantes_pct, 25.00) as penetracion_internet_habitantes

    from ign
    left join indec
        on ign.cod_departamento = indec.cod_departamento
    left join regiones reg
        on ign.cod_departamento = reg.cod_departamento
    left join enacom
        on ign.cod_provincia = enacom.cod_provincia
)

select * from joined
