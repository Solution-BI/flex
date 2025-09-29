USE SCHEMA COP_DSP_FLEX{{uid}};

create or replace view R_FLX_CAT_TYP_DIM_GRP(
	CAT_TYP_DIM_GRP_COD,
	CAT_TYP_DIM_GRP_DSC
) as
select
    flx_grp_cod as cat_typ_dim_grp_cod,
    flx_grp_dsc as cat_typ_dim_grp_dsc
from cop_dmt_flx.r_flx_grp_dim
where flx_dim_cod = 'CAT_TYP'
;