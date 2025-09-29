USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE VIEW V_ETL_R_FLX_GRP_IND
(      IND_GRP_IND                             COMMENT 'Indicator Group Code'
      ,IND_ELM_COD                             COMMENT 'Indicator Element Code'
      ,IND_GRP_RAT_FLG                         COMMENT 'Indicator Group Ratio Flag (1 : Yes / 0 : No)'
      ,IND_NUM_FLG                             COMMENT 'Indicator Numerator Flag (1 : Yes / 0 : No)'
      ,IND_DEN_FLG                             COMMENT 'Indicator Denominator Flag (1 : Yes / 0 : No)'
      ,T_REC_UPD_TST                           COMMENT '[Technical] Last modification date/time'
)COMMENT = '[FLEX] Indicator/KPI Group'
AS
with
base_inds as (
    -- All base indicators should come directly from CC
    -- Writeback to CC will be done based on this level
    select ind_elm_cod
    from cop_dmt_flx.r_flx_ind
    where coalesce(ind_fml_txt, '') = '' and
          ind_elm_cod not in (select distinct par_ind_elm_cod from cop_dmt_flx.r_flx_ind)
    union all
    -- Exception with the Volume: the CC base KPI (unit) depends on the company
    select 'VOL'

),
dep_inds as (

    -- Indicators that are part of the P&L hierarchy
    select par_ind_elm_cod as ind_grp_cod,
           ind_elm_cod     as ind_elm_cod,
           0               as ind_grp_rat_flg,
           1               as ind_num_flg,
           0               as ind_den_flg
    from cop_dmt_flx.r_flx_ind
    where coalesce(par_ind_elm_cod, '') <> ''
    
    union
    -- Indicators calculated from others (e.g. ratios, but not only)
    select r_flx_ind.ind_elm_cod as ind_grp_cod,
           ope_ind.ind_elm_cod,
           -- For ratios, only the numerator will be used to init/impact the Flex scenario
           case when contains(r_flx_ind.ind_fml_txt, ' / ') then 1 else 0 end                   as ind_grp_rat_flg,
           case when contains(r_flx_ind.ind_fml_txt, ' / ') and ope.index > 1 then 0 else 1 end as ind_num_flg,
           case when contains(r_flx_ind.ind_fml_txt, ' / ') and ope.index > 1 then 1 else 0 end as ind_den_flg,
    from cop_dmt_flx.r_flx_ind,
         lateral strtok_split_to_table(r_flx_ind.ind_fml_txt, '+-*/|') as ope,
         cop_dmt_flx.r_flx_ind as ope_ind
    where coalesce(r_flx_ind.ind_fml_txt, '') <> '' and
          trim(ope.value) = ope_ind.ind_elm_cod

),
map_ind_to_base as (

    select ind_elm_cod  as ind_grp_cod,
           ind_elm_cod  as ind_elm_cod,
           0            as ind_grp_rat_flg,
           1            as ind_num_flg,
           0            as ind_den_flg
    from base_inds
    
    union all

    select dep_inds.ind_grp_cod,
           map_ind_to_base.ind_elm_cod,
           dep_inds.ind_grp_rat_flg,
           dep_inds.ind_num_flg,
           dep_inds.ind_den_flg
    from dep_inds
    join map_ind_to_base on (
        dep_inds.ind_elm_cod = map_ind_to_base.ind_grp_cod
    )

)
select ind_grp_cod                as ind_grp_cod,
       ind_elm_cod                as ind_elm_cod,
       any_value(ind_grp_rat_flg) as ind_grp_rat_flg,
       max(ind_num_flg)           as ind_num_flg,
       max(ind_den_flg)           as ind_den_flg,
       TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP) as t_rec_upd_tst
from map_ind_to_base
group by ind_grp_cod,
         ind_elm_cod
order by 1, 2;