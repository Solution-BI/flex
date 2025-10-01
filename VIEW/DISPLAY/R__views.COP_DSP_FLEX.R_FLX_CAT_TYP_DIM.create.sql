use schema cop_dsp_flex;

create or replace view
    r_flx_cat_typ_dim
as
select distinct
    r_flx_grp_cat_typ.cbu_cod,
    r_flx_grp_cat_typ.cat_typ_dim_grp_cod,
    r_flx_cat_typ_dim_grp.cat_typ_dim_grp_dsc,
    r_flx_grp_cat_typ.cat_typ_grp_cod,
    r_flx_grp_cat_typ.cat_typ_grp_dsc
from cop_dmt_flx.r_flx_grp_cat_typ as r_flx_grp_cat_typ
join cop_dsp_flex.r_flx_cat_typ_dim_grp as r_flx_cat_typ_dim_grp on (
    r_flx_cat_typ_dim_grp.cat_typ_dim_grp_cod = r_flx_grp_cat_typ.cat_typ_dim_grp_cod
)
order by 1, 2, 4;
