USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE FUNCTION Fct_Get_Indicator ( P_CBU VARCHAR(10),P_ACC_ELM_COD  VARCHAR(50),P_DST_ELM_COD VARCHAR(30),P_FCT_ARE_ELM_COD VARCHAR(30) )
RETURNS VARCHAR
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Function Script to retreive indicator
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 27-02-2025                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

SELECT   DISTINCT
         COALESCE(ind_elm_cod,'#ERR') ind_elm_cod
FROM     (SELECT P_CBU              CBU_COD
                ,P_ACC_ELM_COD      ACC_ELM_COD
                ,P_DST_ELM_COD      DST_ELM_COD
                ,P_FCT_ARE_ELM_COD  FCT_ARE_ELM_COD
         ) AS rec
         LEFT OUTER JOIN R_FLX_ACCOUNT AS acc on 
         (
            rec.cbu_cod     = acc.cbu_cod AND
            rec.ACC_ELM_COD = acc.ACC_ELM_COD
         )
         -- left JOIN necessary because of missing dest in the lookup (e.g. DCH/DST_4500_C551010)
         LEFT OUTER JOIN R_FLX_DESTINATION AS dst on 
         (
            rec.cbu_cod     = dst.cbu_cod     AND
            rec.DST_ELM_COD = dst.DST_ELM_COD
         )
         LEFT OUTER JOIN R_FLX_FCT_ARE AS fa on 
         (
            rec.cbu_cod         = fa.cbu_cod         AND
            rec.FCT_ARE_ELM_COD = fa.FCT_ARE_ELM_COD
         )
         LEFT OUTER JOIN (SELECT cfg.ind_elm_cod
                                ,cfg.cfg_mod_flg
                                ,(CASE WHEN cfg.acc01_l5_gl_account_code     LIKE '!%' THEN 0 else 1 END) AS acc01_l5_flg
                                ,(CASE WHEN cfg.acc01_l5_gl_account_code NOT LIKE '!%' THEN cfg.acc01_l5_gl_account_code END) AS acc01_l5
                                ,(CASE WHEN cfg.acc01_l5_gl_account_code     LIKE '!%' THEN split(ltrim(cfg.acc01_l5_gl_account_code,  '!'), ',') END) AS acc01_l5_excl

                                ,(CASE WHEN cfg.acc01_l3_sub_category_code     LIKE '!%' THEN 0 else 1 END) AS acc01_l3_flg
                                ,(CASE WHEN cfg.acc01_l3_sub_category_code NOT LIKE '!%' THEN cfg.acc01_l3_sub_category_code END) AS acc01_l3
                                ,(CASE WHEN cfg.acc01_l3_sub_category_code     LIKE '!%' THEN split(ltrim(cfg.acc01_l3_sub_category_code,  '!'), ',') END) AS acc01_l3_excl

                                ,(CASE WHEN cfg.acc01_l1_macro_category_code     LIKE '!%' THEN 0 else 1 END) AS acc01_l1_flg
                                ,(CASE WHEN cfg.acc01_l1_macro_category_code NOT LIKE '!%' THEN cfg.acc01_l1_macro_category_code END) AS acc01_l1
                                ,(CASE WHEN cfg.acc01_l1_macro_category_code     LIKE '!%' THEN split(ltrim(cfg.acc01_l1_macro_category_code,  '!'), ',') END) AS acc01_l1_excl

                                ,(CASE WHEN cfg.acc01_account_type_code     LIKE '!%' THEN 0 else 1 END) AS acc01_type_flg
                                ,(CASE WHEN cfg.acc01_account_type_code NOT LIKE '!%' THEN cfg.acc01_account_type_code END) AS acc01_type
                                ,(CASE WHEN cfg.acc01_account_type_code     LIKE '!%' THEN split(ltrim(cfg.acc01_account_type_code,  '!'), ',') END) AS acc01_type_excl

                                ,(CASE WHEN cfg.acc02_acc_top_nod     LIKE '!%' THEN 0 else 1 END) AS acc02_top_flg
                                ,(CASE WHEN cfg.acc02_acc_top_nod NOT LIKE '!%' THEN cfg.acc02_acc_top_nod END) AS acc02_top
                                ,(CASE WHEN cfg.acc02_acc_top_nod     LIKE '!%' THEN split(ltrim(cfg.acc02_acc_top_nod,  '!'), ',') END) AS acc02_top_excl

                                ,(CASE WHEN cfg.acc03_acc_top_nod     LIKE '!%' THEN 0 else 1 END) AS acc03_top_flg
                                ,(CASE WHEN cfg.acc03_acc_top_nod NOT LIKE '!%' THEN cfg.acc03_acc_top_nod END) AS acc03_top
                                ,(CASE WHEN cfg.acc03_acc_top_nod     LIKE '!%' THEN split(ltrim(cfg.acc03_acc_top_nod,  '!'), ',') END) AS acc03_top_excl

                                ,(CASE WHEN cfg.l1_customer_distribution_channel_desc     LIKE '!%' THEN 0 else 1 END) AS cus_l1_dsc_flg
                                ,(CASE WHEN cfg.l1_customer_distribution_channel_desc NOT LIKE '!%' THEN cfg.l1_customer_distribution_channel_desc END) AS cus_l1_dsc
                                ,(CASE WHEN cfg.l1_customer_distribution_channel_desc     LIKE '!%' THEN split(ltrim(cfg.l1_customer_distribution_channel_desc,  '!'), ',') END) AS cus_l1_dsc_excl

                                ,(CASE WHEN cfg.dst01_l3_destination_code     LIKE '!%' THEN 0 else 1 END) AS dst01_l3_flg
                                ,(CASE WHEN cfg.dst01_l3_destination_code NOT LIKE '!%' THEN cfg.dst01_l3_destination_code END) AS dst01_l3
                                ,(CASE WHEN cfg.dst01_l3_destination_code     LIKE '!%' THEN split(ltrim(cfg.dst01_l3_destination_code,  '!'), ',') END) AS dst01_l3_excl

                                ,(CASE WHEN cfg.dst01_l2_destination_code     LIKE '!%' THEN 0 else 1 END) AS dst01_l2_flg
                                ,(CASE WHEN cfg.dst01_l2_destination_code NOT LIKE '!%' THEN cfg.dst01_l2_destination_code END) AS dst01_l2
                                ,(CASE WHEN cfg.dst01_l2_destination_code     LIKE '!%' THEN split(ltrim(cfg.dst01_l2_destination_code,  '!'), ',') END) AS dst01_l2_excl

                                ,(CASE WHEN cfg.dst01_l1_destination_code     LIKE '!%' THEN 0 else 1 END) AS dst01_l1_flg
                                ,(CASE WHEN cfg.dst01_l1_destination_code NOT LIKE '!%' THEN cfg.dst01_l1_destination_code END) AS dst01_l1
                                ,(CASE WHEN cfg.dst01_l1_destination_code     LIKE '!%' THEN split(ltrim(cfg.dst01_l1_destination_code,  '!'), ',') END) AS dst01_l1_excl

                                ,(CASE WHEN cfg.fct_are_elm_cod     LIKE '!%' THEN 0 else 1 END) AS fct_are_flg
                                ,(CASE WHEN cfg.fct_are_elm_cod NOT LIKE '!%' THEN cfg.fct_are_elm_cod END) AS fct_are
                                ,(CASE WHEN cfg.fct_are_elm_cod     LIKE '!%' THEN split(ltrim(cfg.fct_are_elm_cod,  '!'), ',') END) AS fct_are_excl

                                ,(CASE WHEN cfg.fct_are_lvl1_nod     LIKE '!%' THEN 0 else 1 END) AS fct_are_l1_flg
                                ,(CASE WHEN cfg.fct_are_lvl1_nod NOT LIKE '!%' THEN cfg.fct_are_lvl1_nod END) AS fct_are_l1
                                ,(CASE WHEN cfg.fct_are_lvl1_nod     LIKE '!%' THEN split(ltrim(cfg.fct_are_lvl1_nod,  '!'), ',') END) AS fct_are_l1_excl

                                ,(CASE WHEN cfg.iom_typ_cod     LIKE '!%' THEN 0 else 1 END) AS iom_typ_flg
                                ,(CASE WHEN cfg.iom_typ_cod NOT LIKE '!%' THEN cfg.iom_typ_cod END) AS iom_typ
                                ,(CASE WHEN cfg.iom_typ_cod     LIKE '!%' THEN split(ltrim(cfg.iom_typ_cod,  '!'), ',') END) AS iom_typ_excl

                          FROM   P_FLX_IND_CFG AS cfg
                          WHERE  cfg.t_rec_dlt_flg = 0) AS ind_cfg on 
         (
             (   -- If the field is configured with a list of exclusion, check that the current value does NOT belong in the exclusion list
                 (ind_cfg.acc01_l5_flg = 0 AND NOT array_contains(acc.acc01_l5_gl_account_code::VARIANT, ind_cfg.acc01_l5_excl)) OR
                 -- If the field is configured with a list of inclusion, st ANDard join happens
                 -- If the configuration for the field is null, all values are accepted
                 (ind_cfg.acc01_l5_flg = 1 AND acc.acc01_l5_gl_account_code IS NOT DISTINCT FROM COALESCE(ind_cfg.acc01_l5, acc.acc01_l5_gl_account_code))
             )  AND
             (
                 (ind_cfg.acc01_l3_flg = 0 AND NOT array_contains(acc.acc01_l3_sub_category_code::VARIANT, ind_cfg.acc01_l3_excl)) OR
                 (ind_cfg.acc01_l3_flg = 1 AND acc.acc01_l3_sub_category_code IS NOT DISTINCT FROM COALESCE(ind_cfg.acc01_l3, acc.acc01_l3_sub_category_code))
             )  AND
             (
                 (ind_cfg.acc01_l1_flg = 0 AND NOT array_contains(acc.acc01_l1_macro_category_code::VARIANT, ind_cfg.acc01_l1_excl)) OR
                 (ind_cfg.acc01_l1_flg = 1 AND acc.acc01_l1_macro_category_code IS NOT DISTINCT FROM COALESCE(ind_cfg.acc01_l1, acc.acc01_l1_macro_category_code))
             )  AND
             (
                 (ind_cfg.acc01_type_flg = 0 AND NOT array_contains(acc.acc01_account_type_code::VARIANT, ind_cfg.acc01_type_excl)) OR
                 (ind_cfg.acc01_type_flg = 1 AND acc.acc01_account_type_code IS NOT DISTINCT FROM COALESCE(ind_cfg.acc01_type, acc.acc01_account_type_code))
             )  AND
             (
                 (ind_cfg.acc02_top_flg = 0 AND NOT array_contains(acc.acc02_acc_top_nod::VARIANT, ind_cfg.acc02_top_excl)) OR
                 (ind_cfg.acc02_top_flg = 1 AND acc.acc02_acc_top_nod IS NOT DISTINCT FROM COALESCE(ind_cfg.acc02_top, acc.acc02_acc_top_nod))
             )  AND
             (
                 (ind_cfg.acc03_top_flg = 0 AND NOT array_contains(acc.acc03_acc_top_nod::VARIANT, ind_cfg.acc03_top_excl)) OR
                 (ind_cfg.acc03_top_flg = 1 AND acc.acc03_acc_top_nod IS NOT DISTINCT FROM COALESCE(ind_cfg.acc03_top, acc.acc03_acc_top_nod))
             )  AND
             (
                 (ind_cfg.dst01_l3_flg = 0 AND NOT array_contains(dst.dst01_l3_destination_code::VARIANT, ind_cfg.dst01_l3_excl)) OR
                 (ind_cfg.dst01_l3_flg = 1 AND dst.dst01_l3_destination_code IS NOT DISTINCT FROM COALESCE(ind_cfg.dst01_l3, dst.dst01_l3_destination_code))
             )  AND
             (
                 (ind_cfg.dst01_l2_flg = 0 AND NOT array_contains(dst.dst01_l2_destination_code::VARIANT, ind_cfg.dst01_l2_excl)) OR
                 (ind_cfg.dst01_l2_flg = 1 AND dst.dst01_l2_destination_code IS NOT DISTINCT FROM COALESCE(ind_cfg.dst01_l2, dst.dst01_l2_destination_code))
             )  AND
             (
                 (ind_cfg.dst01_l1_flg = 0 AND NOT array_contains(dst.dst01_l1_destination_code::VARIANT, ind_cfg.dst01_l1_excl)) OR
                 (ind_cfg.dst01_l1_flg = 1 AND dst.dst01_l1_destination_code IS NOT DISTINCT FROM COALESCE(ind_cfg.dst01_l1, dst.dst01_l1_destination_code))
             )  AND
             (
                 (ind_cfg.fct_are_flg = 0 AND NOT array_contains(fa.fct_are_elm_cod::VARIANT, ind_cfg.fct_are_excl)) OR
                 (ind_cfg.fct_are_flg = 1 AND fa.fct_are_elm_cod IS NOT DISTINCT FROM COALESCE(ind_cfg.fct_are, fa.fct_are_elm_cod))
             )  AND
             (
                 (ind_cfg.fct_are_l1_flg = 0 AND NOT array_contains(fa.lvl1_nod::VARIANT, ind_cfg.fct_are_l1_excl)) OR
                 (ind_cfg.fct_are_l1_flg = 1 AND fa.lvl1_nod IS NOT DISTINCT FROM COALESCE(ind_cfg.fct_are_l1, fa.lvl1_nod))
             ) 
    )

$$
;