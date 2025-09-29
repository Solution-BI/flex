use schema cop_dsp_flex{{uid}};

create or replace view
    r_flx_sal_sup_dim_grp
as
select
    flx_grp_cod as sal_sup_dim_grp_cod,
    flx_grp_dsc as sal_sup_dim_grp_dsc
from cop_dmt_flx.r_flx_grp_dim
where flx_dim_cod = 'SAL_SUP'
;
