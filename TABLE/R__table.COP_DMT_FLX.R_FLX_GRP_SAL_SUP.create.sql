use schema cop_dmt_flx{{uid}};

create or replace table
    r_flx_grp_sal_sup
(
    cbu_cod             varchar(10)                             comment 'CBU/Market',
    sal_sup_elm_cod     varchar(30)                             comment 'SU/SP Split code',
    sal_sup_dim_grp_cod number(10,0)                            comment 'Agg level ID',
    sal_sup_grp_cod     varchar(30)                             comment 'Agg level element code',
    sal_sup_grp_dsc     varchar(500)                            comment 'Agg level element name',
    
    t_rec_dlt_flg       number(2,0)                             comment '[Technical] Physical deletion flag',
    t_rec_ins_tst       timestamp_tz                            comment '[Technical] Timestamp of first insertion into the table',
    t_rec_upd_tst       timestamp_tz                            comment '[Technical] Timestamp of last update into the table'

) comment = '[Flex] SU/SP Split aggregation levels'
;


insert overwrite all
    into r_flx_grp_sal_sup
        (cbu_cod, sal_sup_elm_cod, sal_sup_dim_grp_cod, sal_sup_grp_cod, sal_sup_grp_dsc, t_rec_dlt_flg, t_rec_ins_tst, t_rec_upd_tst)
        values (cbu_cod, sal_sup_elm_cod, -1, '$TOTAL_SAL_SUP', 'Total SU+SP', t_rec_dlt_flg, t_rec_ins_tst, t_rec_upd_tst)
    into r_flx_grp_sal_sup
        (cbu_cod, sal_sup_elm_cod, sal_sup_dim_grp_cod, sal_sup_grp_cod, sal_sup_grp_dsc, t_rec_dlt_flg, t_rec_ins_tst, t_rec_upd_tst)
        values (cbu_cod, sal_sup_elm_cod,  0, sal_sup_elm_cod, sal_sup_elm_dsc, t_rec_dlt_flg, t_rec_ins_tst, t_rec_upd_tst)

select
    sal_sup.cbu_cod,
    sal_sup.sal_sup_elm_cod,
    sal_sup.sal_sup_elm_dsc,

    0 as t_rec_dlt_flg,
    current_timestamp as t_rec_ins_tst,
    current_timestamp as t_rec_upd_tst
from r_flx_sal_sup as sal_sup
order by cbu_cod, sal_sup_elm_cod
;
