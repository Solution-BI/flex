use schema cop_dsp_flex{{uid}};

create or replace view
    r_flx_sal_sup_dim
as
select distinct
    r_flx_grp_sal_sup.cbu_cod,
    r_flx_grp_sal_sup.sal_sup_dim_grp_cod,
    r_flx_sal_sup_dim_grp.sal_sup_dim_grp_dsc,
    r_flx_grp_sal_sup.sal_sup_grp_cod,
    r_flx_grp_sal_sup.sal_sup_grp_dsc
from cop_dmt_flx.r_flx_grp_sal_sup as r_flx_grp_sal_sup
join cop_dsp_flex.r_flx_sal_sup_dim_grp as r_flx_sal_sup_dim_grp on (
    r_flx_sal_sup_dim_grp.sal_sup_dim_grp_cod = r_flx_grp_sal_sup.sal_sup_dim_grp_cod
)
order by 1, 2, 4;
