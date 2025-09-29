USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE VIEW V_ETL_S_FLX_SCE_INI
AS
SELECT   W_FLX_SCE_SIM__OUT.SCE_ELM_KEY
        ,W_FLX_SCE_SIM__OUT.CBU_COD
        ,W_FLX_SCE_SIM__OUT.SCE_ELM_COD
        ,R_FLX_SCE.SCE_ELM_DSC
        ,R_FLX_SCE.INI_RQT_TST
        ,W_FLX_SCE_SIM__OUT.DTA_YEA_COD
        ,W_FLX_SCE_SIM__OUT.PER_ELM_COD
        ,ACT_PER_FLG                                                             AS PER_ACT_FLG
        ,W_FLX_SCE_SIM__OUT.CBU_COD || '-' || ETI_ELM_COD                        AS ETI_ELM_KEY
        ,W_FLX_SCE_SIM__OUT.CBU_COD || '-' || CUS_ELM_COD                        AS CUS_ELM_KEY
        ,W_FLX_SCE_SIM__OUT.CBU_COD || '-' || W_FLX_SCE_SIM__OUT.PDT_ELM_COD     AS PDT_ELM_KEY
        ,COALESCE(R_FLX_PDT.LV0_PDT_CAT_COD,'NA')                                AS LV0_PDT_CAT_COD
        ,W_FLX_SCE_SIM__OUT.CBU_COD || '-' || CAT_TYP_ELM_COD                    AS CAT_TYP_ELM_KEY
        ,W_FLX_SCE_SIM__OUT.CBU_COD || '-' || EIB_ELM_COD                        AS EIB_ELM_KEY
        ,W_FLX_SCE_SIM__OUT.CBU_COD || '-' || TTY_ELM_COD                        AS TTY_ELM_KEY
        ,W_FLX_SCE_SIM__OUT.CBU_COD || '-' || SAL_SUP_ELM_COD                    AS SAL_SUP_ELM_KEY
        ,W_FLX_SCE_SIM__OUT.CUR_COD
        ,CNV_RAT_VAL
        ,vol_unt_cod
        ,base_vol                                                                AS SRC_VOL
        ,incr_vol                                                                AS INCR_VOL
        ,cmp1_vol                                                                AS CMP_1ST_VOL
        ,cmp2_vol                                                                AS CMP_2ND_VOL
        ,cmp3_vol                                                                AS CMP_3RD_VOL

        ,base_ns                                                                 AS SRC_NS
        ,incr_ns                                                                 AS INCR_NS
        ,cmp1_ns                                                                 AS CMP_1ST_NS
        ,cmp2_ns                                                                 AS CMP_2ND_NS
        ,cmp3_ns                                                                 AS CMP_3RD_NS
        ,W_FLX_SCE_SIM__OUT.var_ns

        ,base_mat_cos
        ,incr_mat_cos
        ,cmp1_mat_cos
        ,cmp2_mat_cos
        ,cmp3_mat_cos
        ,W_FLX_SCE_SIM__OUT.var_mat_cos

        ,base_mat_oth
        ,incr_mat_oth
        ,cmp1_mat_oth
        ,cmp2_mat_oth
        ,cmp3_mat_oth
        ,W_FLX_SCE_SIM__OUT.var_mat_oth
        ,W_FLX_SCE_SIM__OUT.fca_mat_oth

        ,base_manuf_cos
        ,incr_manuf_cos
        ,cmp1_manuf_cos
        ,cmp2_manuf_cos
        ,cmp3_manuf_cos
        ,W_FLX_SCE_SIM__OUT.var_manuf_cos

        ,base_manuf_oth
        ,incr_manuf_oth
        ,cmp1_manuf_oth
        ,cmp2_manuf_oth
        ,cmp3_manuf_oth
        ,W_FLX_SCE_SIM__OUT.var_manuf_oth
        ,W_FLX_SCE_SIM__OUT.fca_manuf_oth

        ,base_log_ftc_ifo
        ,incr_log_ftc_ifo
        ,cmp1_log_ftc_ifo
        ,cmp2_log_ftc_ifo
        ,cmp3_log_ftc_ifo
        ,W_FLX_SCE_SIM__OUT.var_log_ftc_ifo

        ,base_log_usl
        ,incr_log_usl
        ,cmp1_log_usl
        ,cmp2_log_usl
        ,cmp3_log_usl
        ,W_FLX_SCE_SIM__OUT.var_log_usl

        ,base_log_oth
        ,incr_log_oth
        ,cmp1_log_oth
        ,cmp2_log_oth
        ,cmp3_log_oth
        ,W_FLX_SCE_SIM__OUT.var_log_oth
        ,W_FLX_SCE_SIM__OUT.fca_log_oth

        ,base_ap_wrk
        ,incr_ap_wrk
        ,cmp1_ap_wrk
        ,cmp2_ap_wrk
        ,cmp3_ap_wrk

        ,base_ap_non_wrk
        ,incr_ap_non_wrk
        ,cmp1_ap_non_wrk
        ,cmp2_ap_non_wrk
        ,cmp3_ap_non_wrk

        ,base_ap_oth
        ,incr_ap_oth
        ,cmp1_ap_oth
        ,cmp2_ap_oth
        ,cmp3_ap_oth

        ,base_sf
        ,incr_sf
        ,cmp1_sf
        ,cmp2_sf
        ,cmp3_sf

        ,base_hoo_mkt
        ,incr_hoo_mkt
        ,cmp1_hoo_mkt
        ,cmp2_hoo_mkt
        ,cmp3_hoo_mkt

        ,base_hoo_ops
        ,incr_hoo_ops
        ,cmp1_hoo_ops
        ,cmp2_hoo_ops
        ,cmp3_hoo_ops

        ,base_hoo_dbs
        ,incr_hoo_dbs
        ,cmp1_hoo_dbs
        ,cmp2_hoo_dbs
        ,cmp3_hoo_dbs

        ,base_hoo_glfunc
        ,incr_hoo_glfunc
        ,cmp1_hoo_glfunc
        ,cmp2_hoo_glfunc
        ,cmp3_hoo_glfunc

        ,base_rnd
        ,incr_rnd
        ,cmp1_rnd
        ,cmp2_rnd
        ,cmp3_rnd

        ,base_oie                                                                as SRC_OIE
        ,incr_oie                                                                as INCR_OIE
        ,cmp1_oie                                                                as CMP_1ST_OIE
        ,cmp2_oie                                                                as CMP_2ND_OIE
        ,cmp3_oie                                                                as CMP_3RD_OIE
         
        ,W_FLX_SCE_SIM__OUT.t_rec_ins_tst
        ,W_FLX_SCE_SIM__OUT.t_rec_upd_tst

FROM     COP_DMT_FLX.W_FLX_SCE_SIM__OUT
         LEFT OUTER JOIN COP_DMT_FLX.R_FLX_PDT ON
         (
            R_FLX_PDT.PDT_ELM_KEY = W_FLX_SCE_SIM__OUT.CBU_COD || '-' || W_FLX_SCE_SIM__OUT.PDT_ELM_COD
         )
         INNER JOIN COP_DMT_FLX.R_FLX_SCE ON
         (
                W_FLX_SCE_SIM__OUT.SCE_ELM_KEY = R_FLX_SCE.SCE_ELM_KEY
         )
WHERE    R_FLX_SCE.INI_STS_COD = 'in_progress:3'
AND     (SRC_VOL            != 0
OR       CMP_1ST_VOL        != 0
OR       CMP_2ND_VOL        != 0
OR       CMP_3RD_VOL        != 0
OR       SRC_NS             != 0
OR       CMP_1ST_NS         != 0
OR       CMP_2ND_NS         != 0
OR       CMP_3RD_NS         != 0
OR       BASE_MAT_COS       != 0
OR       CMP1_MAT_COS       != 0
OR       CMP2_MAT_COS       != 0
OR       CMP3_MAT_COS       != 0
OR       BASE_MAT_OTH       != 0
OR       CMP1_MAT_OTH       != 0
OR       CMP2_MAT_OTH       != 0
OR       CMP3_MAT_OTH       != 0
OR       BASE_MANUF_COS     != 0
OR       CMP1_MANUF_COS     != 0
OR       CMP2_MANUF_COS     != 0
OR       CMP3_MANUF_COS     != 0
OR       BASE_MANUF_OTH     != 0
OR       CMP1_MANUF_OTH     != 0
OR       CMP2_MANUF_OTH     != 0
OR       CMP3_MANUF_OTH     != 0
OR       BASE_LOG_FTC_IFO   != 0
OR       CMP1_LOG_FTC_IFO   != 0
OR       CMP2_LOG_FTC_IFO   != 0
OR       CMP3_LOG_FTC_IFO   != 0
OR       BASE_LOG_USL       != 0
OR       CMP1_LOG_USL       != 0
OR       CMP2_LOG_USL       != 0
OR       CMP3_LOG_USL       != 0
OR       BASE_LOG_OTH       != 0
OR       CMP1_LOG_OTH       != 0
OR       CMP2_LOG_OTH       != 0
OR       CMP3_LOG_OTH       != 0
OR       BASE_AP_WRK        != 0
OR       CMP1_AP_WRK        != 0
OR       CMP2_AP_WRK        != 0
OR       CMP3_AP_WRK        != 0
OR       BASE_AP_OTH        != 0
OR       CMP1_AP_OTH        != 0
OR       CMP2_AP_OTH        != 0
OR       CMP3_AP_OTH        != 0
OR       BASE_AP_NON_WRK    != 0
OR       CMP1_AP_NON_WRK    != 0
OR       CMP2_AP_NON_WRK    != 0
OR       CMP3_AP_NON_WRK    != 0
OR       BASE_SF            != 0
OR       CMP1_SF            != 0
OR       CMP2_SF            != 0
OR       CMP3_SF            != 0
OR       BASE_HOO_MKT       != 0
OR       CMP1_HOO_MKT       != 0
OR       CMP2_HOO_MKT       != 0
OR       CMP3_HOO_MKT       != 0
OR       BASE_HOO_OPS       != 0
OR       CMP1_HOO_OPS       != 0
OR       CMP2_HOO_OPS       != 0
OR       CMP3_HOO_OPS       != 0
OR       BASE_HOO_DBS       != 0
OR       CMP1_HOO_DBS       != 0
OR       CMP2_HOO_DBS       != 0
OR       CMP3_HOO_DBS       != 0
OR       BASE_HOO_glfunc    != 0
OR       CMP1_HOO_glfunc    != 0
OR       CMP2_HOO_glfunc    != 0
OR       CMP3_HOO_glfunc    != 0
OR       BASE_RND           != 0
OR       CMP1_RND           != 0
OR       CMP2_RND           != 0
OR       CMP3_RND           != 0
OR       SRC_OIE            != 0
OR       CMP_1ST_OIE        != 0
OR       CMP_2ND_OIE        != 0
OR       CMP_3RD_OIE        != 0
        )
;
