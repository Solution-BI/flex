use schema cop_dsp_flex;

create or replace view
    r_flx_eti_dim
as
select distinct
    r_flx_grp_eti.cbu_cod,
    r_flx_grp_eti.eti_dim_grp_cod,
    r_flx_eti_dim_grp.eti_dim_grp_dsc,
    r_flx_grp_eti.eti_grp_cod,
    r_flx_grp_eti.eti_grp_dsc
from cop_dmt_flx.r_flx_grp_eti as r_flx_grp_eti
join cop_dsp_flex.r_flx_eti_dim_grp as r_flx_eti_dim_grp on (
    r_flx_eti_dim_grp.eti_dim_grp_cod = r_flx_grp_eti.eti_dim_grp_cod
)
order by 1, 2, 4;
