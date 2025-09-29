USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE VIEW V_CHK_F_FLX_MAN_SCE
         (ID
         ,MAN_SCE_ELM_KEY
         ,IND_ELM_COD
         ,ACC_ERR_FLG
         ,DST_ERR_FLG
         ,FCT_ARE_ERR_FLG
         ,ETI_ERR_FLG
         ,CUS_ERR_FLG
         ,PDT_ERR_FLG
         ,EIB_ERR_FLG
         ,TTY_ERR_FLG
         ,CAT_TYP_ERR_FLG
         ,PER_ERR_FLG
         ,MAN_ITM_ERR_FLG
         )
AS
WITH ind_cfg AS (
    SELECT cfg.ind_elm_cod      AS cfg_ind_elm_cod
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
    WHERE  cfg.t_rec_dlt_flg = 0
)
SELECT   src.ID
        ,src.MAN_SCE_ELM_KEY
        ,COALESCE(ind_cfg.cfg_ind_elm_cod,'#ERR')                                              AS IND_ELM_COD
        ,(CASE WHEN acc.ACC_ELM_KEY IS NULL THEN 1 ELSE 0 END)                                 AS ACC_ERR_FLG
        ,(CASE WHEN dst.DST_ELM_KEY IS NULL THEN 1 ELSE 0 END)                                 AS DST_ERR_FLG
        ,(CASE WHEN fa.FCT_ARE_ELM_DSC IS NULL THEN 1 ELSE 0 END)                              AS FCT_ARE_ERR_FLG
        ,(CASE WHEN eti.ETI_ELM_KEY IS NULL THEN 1 ELSE 0 END)                                 AS ETI_ERR_FLG
        ,(CASE WHEN cus.CUS_ELM_KEY IS NULL THEN 1 ELSE 0 END)                                 AS CUS_ERR_FLG
        ,(CASE WHEN pdt.PDT_ELM_KEY IS NULL THEN 1 ELSE 0 END)                                 AS PDT_ERR_FLG
        ,(CASE WHEN eib.EIB_ELM_KEY IS NULL THEN 1 ELSE 0 END)                                 AS EIB_ERR_FLG
        ,(CASE WHEN tty.TTY_ELM_KEY IS NULL THEN 1 ELSE 0 END)                                 AS TTY_ERR_FLG
        ,(CASE WHEN ct.CAT_TYP_ELM_KEY IS NULL THEN 1 ELSE 0 END)                              AS CAT_TYP_ERR_FLG
        ,(CASE WHEN CAST(PER_ELM_COD AS NUMBER(2,0)) NOT BETWEEN 1 AND 12 THEN 1 ELSE 0 END)   AS PER_ERR_FLG
        ,(CASE WHEN COALESCE(ind_cfg.cfg_ind_elm_cod,'#ERR') ='#ERR' THEN 1 ELSE 0 END)  +
         (CASE WHEN acc.ACC_ELM_KEY IS NULL THEN 1 ELSE 0 END)      +
         (CASE WHEN dst.DST_ELM_KEY IS NULL THEN 1 ELSE 0 END)      +
         (CASE WHEN fa.FCT_ARE_ELM_DSC IS NULL THEN 1 ELSE 0 END)   +
         (CASE WHEN eti.ETI_ELM_KEY IS NULL THEN 1 ELSE 0 END)      +
         (CASE WHEN cus.CUS_ELM_KEY IS NULL THEN 1 ELSE 0 END)      +
         (CASE WHEN pdt.PDT_ELM_KEY IS NULL THEN 1 ELSE 0 END)      +
         (CASE WHEN eib.EIB_ELM_KEY IS NULL THEN 1 ELSE 0 END)      +
         (CASE WHEN tty.TTY_ELM_KEY IS NULL THEN 1 ELSE 0 END)      +
         (CASE WHEN ct.CAT_TYP_ELM_KEY IS NULL THEN 1 ELSE 0 END)   +
         (CASE WHEN CAST(PER_ELM_COD AS NUMBER(2,0)) NOT BETWEEN 1 AND 12 THEN 1 ELSE 0 END)   AS MAN_ITM_ERR_FLG
FROM     F_FLX_MAN_SCE src
         LEFT OUTER JOIN R_FLX_ETI AS eti on 
         (
            src.cbu_cod     = eti.cbu_cod AND
            src.ETI_ELM_COD = eti.ETI_ELM_COD
         )
         LEFT OUTER JOIN R_FLX_CUS AS cus on 
         (
            src.cbu_cod     = cus.cbu_cod AND
            src.CUS_ELM_COD = cus.CUS_ELM_COD
         )
         LEFT OUTER JOIN R_FLX_PDT AS pdt on 
         (
            src.cbu_cod     = pdt.cbu_cod AND
            src.PDT_ELM_COD = pdt.PDT_ELM_COD
         )
         LEFT OUTER JOIN R_FLX_EIB AS eib on 
         (
            src.cbu_cod     = eib.cbu_cod AND
            src.EIB_ELM_COD = eib.EIB_ELM_COD
         )
         LEFT OUTER JOIN R_FLX_TTY AS tty on 
         (
            src.cbu_cod     = tty.cbu_cod AND
            src.TTY_ELM_COD = tty.TTY_ELM_COD
         )
         LEFT OUTER JOIN R_FLX_CAT_TYP AS ct on 
         (
            src.cbu_cod         = ct.cbu_cod AND
            src.CAT_TYP_ELM_COD = ct.CAT_TYP_ELM_COD
         )
         LEFT OUTER JOIN R_FLX_ACCOUNT AS acc on 
         (
            src.cbu_cod     = acc.cbu_cod AND
            src.ACC_ELM_COD = acc.ACC_ELM_COD
         )
         -- left JOIN necessary because of missing dest in the lookup (e.g. DCH/DST_4500_C551010)
         LEFT OUTER JOIN R_FLX_DESTINATION AS dst on 
         (
            src.cbu_cod     = dst.cbu_cod     AND
            src.DST_ELM_COD = dst.DST_ELM_KEY
         )
         LEFT OUTER JOIN R_FLX_FCT_ARE AS fa on 
         (
            src.cbu_cod         = fa.cbu_cod         AND
            src.FCT_ARE_ELM_COD = fa.FCT_ARE_ELM_COD
         )
         LEFT OUTER JOIN ind_cfg on 
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
;
