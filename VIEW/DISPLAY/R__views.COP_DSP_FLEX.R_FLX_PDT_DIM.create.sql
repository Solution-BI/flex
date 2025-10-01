use schema cop_dsp_flex;

create or replace view
    r_flx_pdt_dim
as
select distinct
    r_flx_grp_pdt.cbu_cod,
    r_flx_grp_pdt.pdt_dim_grp_cod,
    r_flx_pdt_dim_grp.pdt_dim_grp_dsc,
    r_flx_grp_pdt.pdt_grp_cod,
    r_flx_grp_pdt.pdt_grp_dsc
from cop_dmt_flx.r_flx_grp_pdt as r_flx_grp_pdt
join cop_dsp_flex.r_flx_pdt_dim_grp as r_flx_pdt_dim_grp on (
    r_flx_pdt_dim_grp.pdt_dim_grp_cod = r_flx_grp_pdt.pdt_dim_grp_cod
)
order by 1, 2, 4;
