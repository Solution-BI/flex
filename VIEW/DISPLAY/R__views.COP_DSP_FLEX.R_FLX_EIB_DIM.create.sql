use schema cop_dsp_flex{{uid}};

create or replace view
    r_flx_eib_dim
as
select distinct
    r_flx_grp_eib.cbu_cod,
    r_flx_grp_eib.eib_dim_grp_cod,
    r_flx_eib_dim_grp.eib_dim_grp_dsc,
    r_flx_grp_eib.eib_grp_cod,
    r_flx_grp_eib.eib_grp_dsc
from cop_dmt_flx.r_flx_grp_eib as r_flx_grp_eib
join cop_dsp_flex.r_flx_eib_dim_grp as r_flx_eib_dim_grp on (
    r_flx_eib_dim_grp.eib_dim_grp_cod = r_flx_grp_eib.eib_dim_grp_cod
)
order by 1, 2, 4;
