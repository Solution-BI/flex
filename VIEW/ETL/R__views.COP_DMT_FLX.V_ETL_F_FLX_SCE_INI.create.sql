USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE VIEW V_ETL_F_FLX_SCE_INI
AS
SELECT   F_FLX_SCE_INI.SCE_ELM_KEY
        ,F_FLX_SCE_INI.CBU_COD
        ,F_FLX_SCE_INI.SCE_ELM_COD
        ,F_FLX_SCE_INI.DTA_YEA_COD
        ,F_FLX_SCE_INI.PER_ELM_COD
        ,ACT_PER_FLG                                                        AS PER_ACT_FLG
        ,F_FLX_SCE_INI.CBU_COD || '-' || ETI_ELM_COD                        AS ETI_ELM_KEY
        ,F_FLX_SCE_INI.CBU_COD || '-' || CUS_ELM_COD                        AS CUS_ELM_KEY
        ,F_FLX_SCE_INI.CBU_COD || '-' || F_FLX_SCE_INI.PDT_ELM_COD          AS PDT_ELM_KEY
        ,COALESCE(R_FLX_PDT.LV0_PDT_CAT_COD,'NA')                           AS LV0_PDT_CAT_COD
        ,F_FLX_SCE_INI.CBU_COD || '-' || CAT_TYP_ELM_COD                    AS CAT_TYP_ELM_KEY
        ,F_FLX_SCE_INI.CBU_COD || '-' || EIB_ELM_COD                        AS EIB_ELM_KEY
        ,F_FLX_SCE_INI.CBU_COD || '-' || TTY_ELM_COD                        AS TTY_ELM_KEY
        ,F_FLX_SCE_INI.CBU_COD || '-' || SAL_SUP_ELM_COD                    AS SAL_SUP_ELM_KEY
        ,F_FLX_SCE_INI.CUR_COD
        ,CNV_RAT_VAL
        ,vol_unt_cod
        ,base_vol                                                           AS SRC_VOL
        ,incr_vol                                                           AS INC_VOL
        ,cmp1_vol                                                           AS CMP_1ST_VOL
        ,cmp2_vol                                                           AS CMP_2ND_VOL
        ,cmp3_vol                                                           AS CMP_3RD_VOL

        ,base_gs
        ,incr_gs
        ,cmp1_gs
        ,cmp2_gs
        ,cmp3_gs

        ,base_sd
        ,incr_sd
        ,cmp1_sd
        ,cmp2_sd
        ,cmp3_sd

        ,base_dr
        ,incr_dr
        ,cmp1_dr
        ,cmp2_dr
        ,cmp3_dr

        ,base_ts
        ,incr_ts
        ,cmp1_ts
        ,cmp2_ts
        ,cmp3_ts

        ,base_gs + base_sd + base_dr + base_ts                                   as SRC_NS
        ,incr_gs + incr_sd + incr_dr + incr_ts                                   as INC_NS
        ,cmp1_gs + cmp1_sd + cmp1_dr + cmp1_ts                                   as CMP_1ST_NS
        ,cmp2_gs + cmp2_sd + cmp2_dr + cmp2_ts                                   as CMP_2ND_NS
        ,cmp3_gs + cmp3_sd + cmp3_dr + cmp3_ts                                   as CMP_3RD_NS
        ,PCT_NS

        ,base_mat
        ,incr_mat
        ,cmp1_mat
        ,cmp2_mat
        ,cmp3_mat

        ,base_mat_fix
        ,incr_mat_fix
        ,cmp1_mat_fix
        ,cmp2_mat_fix
        ,cmp3_mat_fix

        ,base_mat_var
        ,incr_mat_var
        ,cmp1_mat_var
        ,cmp2_mat_var
        ,cmp3_mat_var

        ,base_manuf
        ,incr_manuf
        ,cmp1_manuf
        ,cmp2_manuf
        ,cmp3_manuf

        ,base_manuf_fix
        ,incr_manuf_fix
        ,cmp1_manuf_fix
        ,cmp2_manuf_fix
        ,cmp3_manuf_fix

        ,base_manuf_var
        ,incr_manuf_var
        ,cmp1_manuf_var
        ,cmp2_manuf_var
        ,cmp3_manuf_var

        ,base_log
        ,incr_log
        ,cmp1_log
        ,cmp2_log
        ,cmp3_log

        ,base_log_fix
        ,incr_log_fix
        ,cmp1_log_fix
        ,cmp2_log_fix
        ,cmp3_log_fix

        ,base_log_var
        ,incr_log_var
        ,cmp1_log_var
        ,cmp2_log_var
        ,cmp3_log_var

        ,conf_cogs_tot_fca                                                       as SRC_FCA
        ,incr_fca                                                                as INC_FCA

        ,base_mat + base_manuf + base_log                                        as SRC_COGS
        ,incr_mat + incr_manuf + incr_log                                        as inc_cogs
        ,cmp1_mat + cmp1_manuf + cmp1_log                                        as CMP_1ST_COGS
        ,cmp2_mat + cmp2_manuf + cmp2_log                                        as CMP_2ND_COGS
        ,cmp3_mat + cmp3_manuf + cmp3_log                                        as CMP_3RD_COGS

        ,base_mat_var + base_manuf_var + base_log_var                            as SRC_VCOGS
        ,incr_mat_var + incr_manuf_var + incr_log_var                            as inc_vcogs
        ,cmp1_mat_var + cmp1_manuf_var + cmp1_log_var                            as CMP_1ST_VCOGS
        ,cmp2_mat_var + cmp2_manuf_var + cmp2_log_var                            as CMP_2ND_VCOGS
        ,cmp3_mat_var + cmp3_manuf_var + cmp3_log_var                            as CMP_3RD_VCOGS
        ,PCT_VCOGS

/*
        ,SRC_COGS - SRC_vcogs                                                    as SRC_FCOGS
        ,inc_cogs - inc_vcogs                                                    as INC_FCOGS
        ,CMP_1ST_COGS - CMP_1ST_VCOGS                                            as CMP_1ST_FCOGS
        ,CMP_2ND_COGS - CMP_2ND_VCOGS                                            as CMP_2ND_FCOGS
        ,CMP_3RD_COGS - CMP_3RD_VCOGS                                            as CMP_3RD_FCOGS
*/

        ,base_mat_fix + base_manuf_fix + base_log_fix                            as SRC_FCOGS
        ,incr_mat_fix + incr_manuf_fix + incr_log_fix                            as INC_FCOGS
        ,cmp1_mat_fix + cmp1_manuf_fix + cmp1_log_fix                            as CMP_1ST_FCOGS
        ,cmp2_mat_fix + cmp2_manuf_fix + cmp2_log_fix                            as CMP_2ND_FCOGS
        ,cmp3_mat_fix + cmp3_manuf_fix + cmp3_log_fix                            as CMP_3RD_FCOGS
        ,PCT_FCOGS

        ,base_ap                                                                 as SRC_AP
        ,incr_ap                                                                 as INC_AP
        ,cmp1_ap                                                                 as CMP_1ST_AP
        ,cmp2_ap                                                                 as CMP_2ND_AP
        ,cmp3_ap                                                                 as CMP_3RD_AP

        ,base_sfo
        ,incr_sfo
        ,cmp1_sfo
        ,cmp2_sfo
        ,cmp3_sfo

        ,base_hoo
        ,incr_hoo
        ,cmp1_hoo
        ,cmp2_hoo
        ,cmp3_hoo

        ,base_rni
        ,incr_rni
        ,cmp1_rni
        ,cmp2_rni
        ,cmp3_rni

        ,base_sfo + base_hoo + base_rni                                          as SRC_FC
        ,incr_sfo + incr_hoo + incr_rni                                          as inc_fc
        ,cmp1_sfo + cmp1_hoo + cmp1_rni                                          as CMP_1ST_FC
        ,cmp2_sfo + cmp2_hoo + cmp2_rni                                          as CMP_2ND_FC
        ,cmp3_sfo + cmp3_hoo + cmp3_rni                                          as CMP_3RD_FC

        ,base_oie                                                                as SRC_OIE
        ,incr_oie                                                                as INC_OIE
        ,cmp1_oie                                                                as CMP_1ST_OIE
        ,cmp2_oie                                                                as CMP_2ND_OIE
        ,cmp3_oie                                                                as CMP_3RD_OIE
         
        ,F_FLX_SCE_INI.t_rec_ins_tst
        ,F_FLX_SCE_INI.t_rec_upd_tst
        ,R_FLX_SCE.INI_RQT_TST

FROM     COP_DMT_FLX.F_FLX_SCE_INI
         LEFT OUTER JOIN COP_DMT_FLX.R_FLX_PDT ON
         (
            R_FLX_PDT.PDT_ELM_KEY = F_FLX_SCE_INI.CBU_COD || '-' || F_FLX_SCE_INI.PDT_ELM_COD
         )
         INNER JOIN COP_DMT_FLX.R_FLX_SCE ON
         (
                F_FLX_SCE_INI.SCE_ELM_KEY = R_FLX_SCE.SCE_ELM_KEY
         )
WHERE    R_FLX_SCE.INI_STS_COD = 'in_progress:5'
AND     (SRC_VOL       != 0
OR       CMP_1ST_VOL   != 0
OR       CMP_2ND_VOL   != 0
OR       CMP_3RD_VOL   != 0
OR       SRC_NS        != 0
OR       CMP_1ST_NS    != 0
OR       CMP_2ND_NS    != 0
OR       CMP_3RD_NS    != 0
OR       SRC_COGS      != 0
OR       CMP_1ST_COGS  != 0
OR       CMP_2ND_COGS  != 0
OR       CMP_3RD_COGS  != 0
OR       SRC_VCOGS     != 0
OR       CMP_1ST_VCOGS != 0
OR       CMP_2ND_VCOGS != 0
OR       CMP_3RD_VCOGS != 0
OR       SRC_FCOGS     != 0
OR       CMP_1ST_FCOGS != 0
OR       CMP_2ND_FCOGS != 0
OR       CMP_3RD_FCOGS != 0
OR       SRC_AP        != 0
OR       CMP_1ST_AP    != 0
OR       CMP_2ND_AP    != 0
OR       CMP_3RD_AP    != 0
OR       SRC_OIE       != 0
OR       CMP_1ST_OIE   != 0
OR       CMP_2ND_OIE   != 0
OR       CMP_3RD_OIE   != 0
OR       SRC_FC        != 0
OR       CMP_1ST_FC    != 0
OR       CMP_2ND_FC    != 0
OR       CMP_3RD_FC    != 0
        )
;
