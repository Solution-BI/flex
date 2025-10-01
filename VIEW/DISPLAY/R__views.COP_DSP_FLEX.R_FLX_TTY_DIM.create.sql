use schema cop_dsp_flex;

create or replace view
    r_flx_tty_dim
as
select distinct
    r_flx_grp_tty.cbu_cod,
    r_flx_grp_tty.tty_dim_grp_cod,
    r_flx_tty_dim_grp.tty_dim_grp_dsc,
    r_flx_grp_tty.tty_grp_cod,
    r_flx_grp_tty.tty_grp_dsc
from cop_dmt_flx.r_flx_grp_tty as r_flx_grp_tty
join cop_dsp_flex.r_flx_tty_dim_grp as r_flx_tty_dim_grp on (
    r_flx_tty_dim_grp.tty_dim_grp_cod = r_flx_grp_tty.tty_dim_grp_cod
)
order by 1, 2, 4;
