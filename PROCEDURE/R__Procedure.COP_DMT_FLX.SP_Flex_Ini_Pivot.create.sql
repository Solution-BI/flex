USE DATABASE {{env}}_COP;
USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE PROCEDURE SP_Flex_Ini_Pivot()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in W_FLX_SCE_SIM__OUT
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 12-12-2024                                                 
=========================================================================
 
2025-06-11 N.COQUIO use the ETI_CUR_COD instead of the base source scenario currency code
 
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);
V_RUN_ID         VARCHAR(256);
v_STEP_NUM       NUMBER(5,0) := 0;
V_STEP_BEG_DT    VARCHAR(50);
V_STEP_END_DT    VARCHAR(50);
v_ERR_MSG        VARCHAR(1000);
v_IS_SCENARIO    INTEGER;

v_STS_PROC       VARCHAR(5000);
v_ERR_STEP       NUMBER(2,0);


v_NB_ROW         INTEGER;
v_CBU_CODE       VARCHAR(10);
v_SCE_ELM_COD    VARCHAR(50);
BEGIN
/*
    IF ( P_RUN_ID IS NULL ) THEN
        -- Generate the UUID for the procedure
        CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;
    ELSE
        V_RUN_ID := P_RUN_ID;
    END IF;
*/
    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Ini_Pivot','#','DLT',CURRENT_USER);

    -- 1. Truncate the table W_FLX_SCE_SIM__OUT
    v_STEP_TABLE := 'TRUNCATE W_FLX_SCE_SIM__OUT';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SCE_SIM__OUT;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 2. Load the data in the table W_FLX_SCE_SIM__OUT

    v_STEP_TABLE := 'INSERT W_FLX_SCE_SIM__OUT';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    INSERT INTO COP_DMT_FLX.W_FLX_SCE_SIM__OUT
               (SCE_ELM_KEY
               ,CBU_COD
               ,SCE_ELM_COD
               ,DTA_YEA_COD
               ,PER_ELM_COD
               ,ACT_PER_FLG
               ,ETI_ELM_COD
               ,CUS_ELM_COD
               ,PDT_ELM_COD
               ,CAT_TYP_ELM_COD
               ,EIB_ELM_COD
               ,TTY_ELM_COD
               ,SAL_SUP_ELM_COD
               ,CUR_COD
               ,CNV_RAT_VAL
--               ,vol_unt_cod
               ,base_vol
               ,incr_vol
               ,cmp1_vol
               ,cmp2_vol
               ,cmp3_vol
               ,base_ns
               ,incr_ns
               ,cmp1_ns
               ,cmp2_ns
               ,cmp3_ns
               ,base_mat_cos
               ,incr_mat_cos
               ,cmp1_mat_cos
               ,cmp2_mat_cos
               ,cmp3_mat_cos
               ,base_mat_oth
               ,incr_mat_oth
               ,cmp1_mat_oth
               ,cmp2_mat_oth
               ,cmp3_mat_oth
               ,base_manuf_cos
               ,incr_manuf_cos
               ,cmp1_manuf_cos
               ,cmp2_manuf_cos
               ,cmp3_manuf_cos
               ,base_manuf_oth
               ,incr_manuf_oth
               ,cmp1_manuf_oth
               ,cmp2_manuf_oth
               ,cmp3_manuf_oth
               ,base_log_ftc_ifo
               ,incr_log_ftc_ifo
               ,cmp1_log_ftc_ifo
               ,cmp2_log_ftc_ifo
               ,cmp3_log_ftc_ifo
               ,base_log_usl
               ,incr_log_usl
               ,cmp1_log_usl
               ,cmp2_log_usl
               ,cmp3_log_usl
               ,base_log_oth
               ,incr_log_oth
               ,cmp1_log_oth
               ,cmp2_log_oth
               ,cmp3_log_oth
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
               ,base_oie
               ,incr_oie
               ,cmp1_oie
               ,cmp2_oie
               ,cmp3_oie
               ,fca_mat_oth
               ,fca_manuf_oth
               ,fca_log_oth
               ,var_ns
               ,var_mat_cos
               ,var_mat_oth
               ,var_manuf_cos
               ,var_manuf_oth
               ,var_log_ftc_ifo
               ,var_log_usl
               ,var_log_oth
               ,t_rec_ins_tst
               ,t_rec_upd_tst
               )
--------------------------------------------------------------------------------
    WITH UNPIVOT_SOURCE_SCENARIOS AS (
        /* Retreive all the comparable scenarios */
        SELECT   CBU_COD
                ,SCE_ELM_COD
                ,(CASE COLNAME
                       WHEN 'CMP_1ST_SRC_SCE_COD' THEN 'CMP1'
                       WHEN 'CMP_2ND_SRC_SCE_COD' THEN 'CMP2'
                       WHEN 'CMP_3RD_SRC_SCE_COD' THEN 'CMP3'
                  END)                                           AS SRC_SCE_TYP_COD
                ,'CMP_SCE'                                       AS SCE_SRC_TAB_COD
                ,SRC_SCE_ELM_COD                                 AS SRC_SCE_ELM_COD
        FROM     COP_DMT_FLX.R_FLX_SCE
                 UNPIVOT INCLUDE NULLS (SRC_SCE_ELM_COD FOR COLNAME IN (CMP_1ST_SRC_SCE_COD
                                                                       ,CMP_2ND_SRC_SCE_COD
                                                                       ,CMP_3RD_SRC_SCE_COD))
        WHERE    R_FLX_SCE.T_REC_DLT_FLG      = 0
        AND      R_FLX_SCE.INI_STS_COD   NOT IN ('created','done','failed','requested')
        UNION
        /* Retreive all the base scenarios included the scenario configured by KPI*/
        SELECT   DISTINCT 
                 R_FLX_SCE.CBU_COD
                ,R_FLX_SCE.SCE_ELM_COD
                ,'BASE'                                          AS SRC_SCE_TYP_COD
                ,'SRC_SCE'                                       AS SCE_SRC_TAB_COD
                ,P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD               AS SRC_SCE_ELM_COD
        FROM     COP_DMT_FLX.R_FLX_SCE
                 LEFT OUTER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON
                 (
                      R_FLX_SCE.SCE_ELM_KEY = P_FLX_SCE_CFG_IND.SCE_ELM_KEY
                 )
        WHERE    R_FLX_SCE.T_REC_DLT_FLG = 0
        AND      R_FLX_SCE.INI_STS_COD   NOT IN ('created','done','failed','requested')
        UNION
        SELECT   CBU_COD
                ,SCE_ELM_COD
                ,'BASE'                                          AS SRC_SCE_TYP_COD
                ,'SRC_SCE'                                       AS SCE_SRC_TAB_COD
                ,ACT_SRC_SCE_COD                                 AS SRC_SCE_ELM_COD
        FROM     COP_DMT_FLX.R_FLX_SCE
        WHERE    R_FLX_SCE.T_REC_DLT_FLG        = 0
        AND      R_FLX_SCE.UPD_ACT_FLG          = 1
        AND      R_FLX_SCE.ACT_SRC_SCE_COD     IS NOT NULL
        AND      R_FLX_SCE.INI_STS_COD     NOT IN ('created','done','failed','requested')
        UNION
        SELECT   CBU_COD
                ,SCE_ELM_COD
                ,'BASE'                                          AS SRC_SCE_TYP_COD
                ,'SRC_SCE'                                       AS SCE_SRC_TAB_COD
                ,'CLOSE_GAP'                                     AS SRC_SCE_ELM_COD
        FROM     COP_DMT_FLX.R_FLX_SCE
        WHERE    R_FLX_SCE.T_REC_DLT_FLG        = 0
        AND      R_FLX_SCE.UPD_ACT_FLG          = 1
        AND      R_FLX_SCE.ACT_SRC_SCE_COD     IS NOT NULL
        AND      R_FLX_SCE.INI_STS_COD     NOT IN ('created','done','failed','requested')
    )    
--------------------------------------------------------------------------------
/*
   ,ETI_CUR_COD AS (
        SELECT   sce.CBU_COD
                ,src_data.ETI_ELM_COD
                ,src_data.SRC_CUR_COD
        FROM     COP_DMT_FLX.R_FLX_SCE as sce
                 INNER JOIN unpivot_source_scenarios as src_sce ON 
                 (
                      sce.CBU_COD             = src_sce.CBU_COD     AND
                      sce.SCE_ELM_COD         = src_sce.SCE_ELM_COD AND
                      src_sce.SRC_SCE_TYP_COD = 'BASE'
                  )
                  INNER JOIN COP_DMT_FLX.V_ETL_FLX_SRC_CMP_SCE as src_data ON
                  (
                      src_sce.CBU_COD          = src_data.CBU_COD         AND
                      src_sce.SCE_ELM_COD      = src_data.SCE_ELM_COD     AND
                      src_sce.SRC_SCE_ELM_COD  = src_data.SRC_SCE_ELM_COD
                  )
         WHERE    src_data.SRC_CUR_COD     != 'NA'
         AND      sce.INI_STS_COD      NOT IN ('created','done','failed','requested')
         GROUP BY sce.CBU_COD
                 ,src_data.ETI_ELM_COD
                 ,src_data.SRC_CUR_COD
        QUALIFY RANK() OVER (PARTITION BY sce.CBU_COD
                                         ,src_data.ETI_ELM_COD
                                          ORDER BY src_data.SRC_CUR_COD
                                                  ,COUNT(*) DESC) = 1
      )
--------------------------------------------------------------------------------
*/
   ,AGG_SOURCE_DATA as (
        SELECT   sce.SCE_ELM_KEY
                ,sce.CBU_COD
                ,sce.SCE_ELM_COD
                ,sce.DTA_YEA_COD
                ,src_data.PER_ELM_COD                                                      AS PER_ELM_COD
                ,ANY_VALUE(IFF(src_data.PER_ELM_COD <= sce.LST_ACT_PER_COD, 1, 0))         AS ACT_PER_FLG
                ,src_data.ETI_ELM_COD                                                      AS ETI_ELM_COD
                ,cus.CUS_GRP_COD                                                           AS CUS_ELM_COD
                ,src_data.PDT_ELM_COD                                                      AS PDT_ELM_COD
                ,src_data.CAT_TYP_ELM_COD                                                  AS CAT_TYP_ELM_COD
                ,src_data.EIB_ELM_COD                                                      AS EIB_ELM_COD
                ,src_data.TTY_ELM_COD                                                      AS TTY_ELM_COD
                ,src_data.SAL_SUP_ELM_COD                                                  AS SAL_SUP_ELM_COD
                ,sce.CUR_COD                                                               AS CUR_COD
                ,sce.VAR_NS                                                                AS VAR_NS
                ,sce.VAR_MAT_COS                                                           AS VAR_MAT_COS
                ,sce.VAR_MAT_OTH                                                           AS VAR_MAT_OTH
                ,sce.VAR_MANUF_COS                                                         AS VAR_MANUF_COS
                ,sce.VAR_MANUF_OTH                                                         AS VAR_MANUF_OTH
                ,sce.VAR_LOG_FTC_IFO                                                       AS VAR_LOG_FTC_IFO
                ,sce.VAR_LOG_USL                                                           AS VAR_LOG_USL
                ,sce.VAR_LOG_OTH                                                           AS VAR_LOG_OTH
                ,COALESCE(COALESCE(fca.FCA_MAT_OTH_VAL,0) / NULLIFZERO(CASE sce.CUR_COD 
                                                                            WHEN 'LC'     THEN 1
                                                                            WHEN 'EUR_CY' THEN COALESCE(RATE_CY,1)
                                                                            WHEN 'EUR_CY' THEN COALESCE(RATE_FY,1)
                                                                       END),0)             AS FCA_MAT_OTH_VAL
                ,COALESCE(COALESCE(fca.FCA_MANUF_OTH_VAL,0) / NULLIFZERO(CASE sce.CUR_COD 
                                                                              WHEN 'LC'     THEN 1
                                                                              WHEN 'EUR_CY' THEN COALESCE(RATE_CY,1)
                                                                              WHEN 'EUR_CY' THEN COALESCE(RATE_FY,1)
                                                                         END),0)           AS FCA_MANUF_OTH_VAL
                ,COALESCE(COALESCE(fca.FCA_LOG_OTH_VAL,0) / NULLIFZERO(CASE sce.CUR_COD 
                                                                            WHEN 'LC'     THEN 1
                                                                            WHEN 'EUR_CY' THEN COALESCE(RATE_CY,1)
                                                                            WHEN 'EUR_CY' THEN COALESCE(RATE_FY,1)
                                                                       END),0)             AS FCA_LOG_OTH_VAL
                ,CAST(src_sce.SRC_SCE_TYP_COD ||'_'|| 
                      src_data.IND_ELM_COD AS VARCHAR(50))                                 AS PVT_COL_COD
                ,SUM(src_data.AMOUNT)                                                      AS amount
--                ,eti.VOL_UNT_COD
                --,src_data.SCE_SRC_TAB_COD
                -- Store applied rate in scenario
                --,any_value(src_data.rate_cy) as rate_cy
                --,any_value(src_data.rate_fy) as rate_fy
        FROM     COP_DMT_FLX.R_FLX_SCE as sce
                 INNER JOIN unpivot_source_scenarios AS src_sce ON 
                 (
                      sce.CBU_COD      = src_sce.CBU_COD     AND
                      sce.SCE_ELM_COD  = src_sce.SCE_ELM_COD
                 )
                 INNER JOIN COP_DMT_FLX.V_ETL_FLX_SRC_CMP_SCE AS src_data ON 
                 (
                      src_sce.CBU_COD          = src_data.CBU_COD           AND
                      src_sce.SCE_ELM_COD      = src_data.SCE_ELM_COD       AND
                      src_sce.SCE_SRC_TAB_COD  = src_data.SCE_SRC_TAB_COD   AND
                      src_sce.SRC_SCE_ELM_COD  = src_data.SRC_SCE_ELM_COD
                 )
                 INNER JOIN COP_DMT_FLX.R_FLX_GRP_CUS AS cus ON 
                 (
                      src_data.CBU_COD      = cus.CBU_COD         AND
                      src_data.CUS_ELM_COD  = cus.cus_elm_cod     AND 
                      sce.cus_dim_grp_cod   = cus.cus_dim_grp_cod
                 )
                 INNER JOIN COP_DMT_FLX.R_FLX_ETI AS eti ON 
                 (
                      src_data.ETI_ELM_COD  = eti.ETI_ELM_COD
                 )
/*
                 INNER JOIN ETI_CUR_COD ON 
                 (
                      ETI_CUR_COD.CBU_COD      = src_data.CBU_COD     AND
                      ETI_CUR_COD.ETI_ELM_COD  = src_data.ETI_ELM_COD
                 )
*/
                 LEFT OUTER JOIN COP_DMT_FLX.R_FLX_PDT AS pdt ON
                 (
                     src_data.CBU_COD      = pdt.CBU_COD     AND
                     src_data.PDT_ELM_COD  = pdt.PDT_ELM_COD
                 )
                 LEFT OUTER JOIN COP_DMT_FLX.R_FLX_ETI_PDT_FCA AS fca ON
                 (
                      src_data.CBU_COD         = fca.CBU_COD     AND
                      src_data.ETI_ELM_COD     = fca.ETI_ELM_COD AND
                      src_data.PDT_ELM_COD     = fca.PDT_ELM_COD
                 )
                 LEFT OUTER JOIN COP_DSP_CONTROLLING_CLOUD.R_CURRENCY_UNIFY_AGG cur_agg ON
                 (
                     cur_agg.CBU_COD = eti.CBU_COD     AND
                     cur_agg.CUR_COD = eti.ETI_CUR_COD
--                     cur_agg.CBU_COD = ETI_CUR_COD.CBU_COD     AND
--                     cur_agg.CUR_COD = ETI_CUR_COD.SRC_CUR_COD
                 )             
        WHERE    1 = 1
        AND      sce.INI_STS_COD      NOT IN ('created','done','failed','requested')
        AND      src_data.IND_ELM_COD     <> 'NA'
        GROUP BY ALL
    )
   ,ALL_ AS (
        SELECT   SCE_ELM_KEY
                ,CBU_COD
                ,SCE_ELM_COD
                ,DTA_YEA_COD
                ,PER_ELM_COD
                ,ACT_PER_FLG
                ,ETI_ELM_COD
                ,CUS_ELM_COD
                ,PDT_ELM_COD
                ,CAT_TYP_ELM_COD
                ,EIB_ELM_COD
                ,TTY_ELM_COD
                ,SAL_SUP_ELM_COD
                ,CUR_COD
                ,1                                                                       as cnv_rat_val
                -- Add into Entity configuration
--                ,VOL_UNT_COD
                ,"'BASE_VOL'"                                                            as base_vol
                ,0                                                                       as incr_vol
                ,"'CMP1_VOL'"                                                            as cmp1_vol
                ,"'CMP2_VOL'"                                                            as cmp2_vol
                ,"'CMP3_VOL'"                                                            as cmp3_vol

                ,"'BASE_NS'"                                                             as base_ns
                ,0                                                                       as incr_ns
                ,"'CMP1_NS'"                                                             as cmp1_ns
                ,"'CMP2_NS'"                                                             as cmp2_ns
                ,"'CMP3_NS'"                                                             as cmp3_ns
                ,"'BASE_MAT_COS'"                                                        as base_mat_cos
                ,0                                                                       as incr_mat_cos
                ,"'CMP1_MAT_COS'"                                                        as cmp1_mat_cos
                ,"'CMP2_MAT_COS'"                                                        as cmp2_mat_cos
                ,"'CMP3_MAT_COS'"                                                        as cmp3_mat_cos

                ,"'BASE_MAT_OTH'"                                                        as base_mat_oth
                ,0                                                                       as incr_mat_oth
                ,"'CMP1_MAT_OTH'"                                                        as cmp1_mat_oth
                ,"'CMP2_MAT_OTH'"                                                        as cmp2_mat_oth
                ,"'CMP3_MAT_OTH'"                                                        as cmp3_mat_oth

                ,"'BASE_MANUF_COS'"                                                      as base_manuf_cos
                ,0                                                                       as incr_manuf_cos
                ,"'CMP1_MANUF_COS'"                                                      as cmp1_manuf_cos
                ,"'CMP2_MANUF_COS'"                                                      as cmp2_manuf_cos
                ,"'CMP3_MANUF_COS'"                                                      as cmp3_manuf_cos

                ,"'BASE_MANUF_OTH'"                                                      as base_manuf_oth
                ,0                                                                       as incr_manuf_oth
                ,"'CMP1_MANUF_OTH'"                                                      as cmp1_manuf_oth
                ,"'CMP2_MANUF_OTH'"                                                      as cmp2_manuf_oth
                ,"'CMP3_MANUF_OTH'"                                                      as cmp3_manuf_oth

                ,"'BASE_LOG_FTC_IFO'"                                                    as base_log_ftc_ifo
                ,0                                                                       as incr_log_ftc_ifo
                ,"'CMP1_LOG_FTC_IFO'"                                                    as cmp1_log_ftc_ifo
                ,"'CMP2_LOG_FTC_IFO'"                                                    as cmp2_log_ftc_ifo
                ,"'CMP3_LOG_FTC_IFO'"                                                    as cmp3_log_ftc_ifo

                ,"'BASE_LOG_USL'"                                                        as base_log_usl
                ,0                                                                       as incr_log_usl
                ,"'CMP1_LOG_USL'"                                                        as cmp1_log_usl
                ,"'CMP2_LOG_USL'"                                                        as cmp2_log_usl
                ,"'CMP3_LOG_USL'"                                                        as cmp3_log_usl

                ,"'BASE_LOG_OTH'"                                                        as base_log_oth
                ,0                                                                       as incr_log_oth
                ,"'CMP1_LOG_OTH'"                                                        as cmp1_log_oth
                ,"'CMP2_LOG_OTH'"                                                        as cmp2_log_oth
                ,"'CMP3_LOG_OTH'"                                                        as cmp3_log_oth

                ,"'BASE_AP_WRK'"                                                         as base_ap_wrk
                ,0                                                                       as incr_ap_wrk
                ,"'CMP1_AP_WRK'"                                                         as cmp1_ap_wrk
                ,"'CMP2_AP_WRK'"                                                         as cmp2_ap_wrk
                ,"'CMP3_AP_WRK'"                                                         as cmp3_ap_wrk

                ,"'BASE_AP_NON_WRK'"                                                     as base_ap_non_wrk
                ,0                                                                       as incr_ap_non_wrk
                ,"'CMP1_AP_NON_WRK'"                                                     as cmp1_ap_non_wrk
                ,"'CMP2_AP_NON_WRK'"                                                     as cmp2_ap_non_wrk
                ,"'CMP3_AP_NON_WRK'"                                                     as cmp3_ap_non_wrk

                ,"'BASE_AP_OTH'"                                                         as base_ap_oth
                ,0                                                                       as incr_ap_oth
                ,"'CMP1_AP_OTH'"                                                         as cmp1_ap_oth
                ,"'CMP2_AP_OTH'"                                                         as cmp2_ap_oth
                ,"'CMP3_AP_OTH'"                                                         as cmp3_ap_oth

                ,"'BASE_SF'"                                                             as base_sf
                ,0                                                                       as incr_sf
                ,"'CMP1_SF'"                                                             as cmp1_sf
                ,"'CMP2_SF'"                                                             as cmp2_sf
                ,"'CMP3_SF'"                                                             as cmp3_sf

                ,"'BASE_HOO_MKT'"                                                        as base_hoo_mkt
                ,0                                                                       as incr_hoo_mkt
                ,"'CMP1_HOO_MKT'"                                                        as cmp1_hoo_mkt
                ,"'CMP2_HOO_MKT'"                                                        as cmp2_hoo_mkt
                ,"'CMP3_HOO_MKT'"                                                        as cmp3_hoo_mkt

                ,"'BASE_HOO_OPS'"                                                        as base_hoo_ops
                ,0                                                                       as incr_hoo_ops
                ,"'CMP1_HOO_OPS'"                                                        as cmp1_hoo_ops
                ,"'CMP2_HOO_OPS'"                                                        as cmp2_hoo_ops
                ,"'CMP3_HOO_OPS'"                                                        as cmp3_hoo_ops

                ,"'BASE_HOO_DBS'"                                                        as base_hoo_dbs
                ,0                                                                       as incr_hoo_dbs
                ,"'CMP1_HOO_DBS'"                                                        as cmp1_hoo_dbs
                ,"'CMP2_HOO_DBS'"                                                        as cmp2_hoo_dbs
                ,"'CMP3_HOO_DBS'"                                                        as cmp3_hoo_dbs

                ,"'BASE_HOO_GLFUNC'"                                                     as base_hoo_glfunc
                ,0                                                                       as incr_hoo_glfunc
                ,"'CMP1_HOO_GLFUNC'"                                                     as cmp1_hoo_glfunc
                ,"'CMP2_HOO_GLFUNC'"                                                     as cmp2_hoo_glfunc
                ,"'CMP3_HOO_GLFUNC'"                                                     as cmp3_hoo_glfunc

                ,"'BASE_RND'"                                                            as base_rnd
                ,0                                                                       as incr_rnd
                ,"'CMP1_RND'"                                                            as cmp1_rnd
                ,"'CMP2_RND'"                                                            as cmp2_rnd
                ,"'CMP3_RND'"                                                            as cmp3_rnd

                ,"'BASE_OIE'"                                                            as base_oie
                ,0                                                                       as incr_oie
                ,"'CMP1_OIE'"                                                            as cmp1_oie
                ,"'CMP2_OIE'"                                                            as cmp2_oie
                ,"'CMP3_OIE'"                                                            as cmp3_oie

                ,FCA_MAT_OTH_VAL                                                         as fca_mat_oth
                ,FCA_MANUF_OTH_VAL                                                       as fca_manuf_oth
                ,FCA_LOG_OTH_VAL                                                         as fca_log_oth

                ,VAR_NS                                                                  as var_ns
                ,VAR_MAT_COS                                                             as var_mat_cos
                ,VAR_MAT_OTH                                                             as var_mat_oth
                ,VAR_MANUF_COS                                                           as var_manuf_cos
                ,VAR_MANUF_OTH                                                           as var_manuf_oth
                ,VAR_LOG_FTC_IFO                                                         as var_log_ftc_ifo
                ,VAR_LOG_USL                                                             as var_log_usl
                ,VAR_LOG_OTH                                                             as var_log_oth

                ,current_timestamp                                                       as t_rec_ins_tst
                ,current_timestamp                                                       as t_rec_upd_tst
        FROM     agg_source_data 
                 PIVOT (SUM(AMOUNT) FOR PVT_COL_COD IN ('BASE_VOL'
                                                       ,'CMP1_VOL'
                                                       ,'CMP2_VOL'
                                                       ,'CMP3_VOL'
                                                       ,'BASE_NS'
                                                       ,'CMP1_NS'
                                                       ,'CMP2_NS'
                                                       ,'CMP3_NS'
                                                       ,'BASE_MAT_COS'
                                                       ,'CMP1_MAT_COS'
                                                       ,'CMP2_MAT_COS'
                                                       ,'CMP3_MAT_COS'
                                                       ,'BASE_MAT_OTH'
                                                       ,'CMP1_MAT_OTH'
                                                       ,'CMP2_MAT_OTH'
                                                       ,'CMP3_MAT_OTH'
                                                       ,'BASE_MANUF_COS'
                                                       ,'CMP1_MANUF_COS'
                                                       ,'CMP2_MANUF_COS'
                                                       ,'CMP3_MANUF_COS'
                                                       ,'BASE_MANUF_OTH'
                                                       ,'CMP1_MANUF_OTH'
                                                       ,'CMP2_MANUF_OTH'
                                                       ,'CMP3_MANUF_OTH'
                                                       ,'BASE_LOG_FTC_IFO'
                                                       ,'CMP1_LOG_FTC_IFO'
                                                       ,'CMP2_LOG_FTC_IFO'
                                                       ,'CMP3_LOG_FTC_IFO'
                                                       ,'BASE_LOG_USL'
                                                       ,'CMP1_LOG_USL'
                                                       ,'CMP2_LOG_USL'
                                                       ,'CMP3_LOG_USL'
                                                       ,'BASE_LOG_OTH'
                                                       ,'CMP1_LOG_OTH'
                                                       ,'CMP2_LOG_OTH'
                                                       ,'CMP3_LOG_OTH'
                                                       ,'BASE_AP_WRK'
                                                       ,'CMP1_AP_WRK'
                                                       ,'CMP2_AP_WRK'
                                                       ,'CMP3_AP_WRK'
                                                       ,'BASE_AP_NON_WRK'
                                                       ,'CMP1_AP_NON_WRK'
                                                       ,'CMP2_AP_NON_WRK'
                                                       ,'CMP3_AP_NON_WRK'
                                                       ,'BASE_AP_OTH'
                                                       ,'CMP1_AP_OTH'
                                                       ,'CMP2_AP_OTH'
                                                       ,'CMP3_AP_OTH'
                                                       ,'BASE_SF'
                                                       ,'CMP1_SF'
                                                       ,'CMP2_SF'
                                                       ,'CMP3_SF'
                                                       ,'BASE_HOO_MKT'
                                                       ,'CMP1_HOO_MKT'
                                                       ,'CMP2_HOO_MKT'
                                                       ,'CMP3_HOO_MKT'
                                                       ,'BASE_HOO_OPS'
                                                       ,'CMP1_HOO_OPS'
                                                       ,'CMP2_HOO_OPS'
                                                       ,'CMP3_HOO_OPS'
                                                       ,'BASE_HOO_DBS'
                                                       ,'CMP1_HOO_DBS'
                                                       ,'CMP2_HOO_DBS'
                                                       ,'CMP3_HOO_DBS'
                                                       ,'BASE_HOO_GLFUNC'
                                                       ,'CMP1_HOO_GLFUNC'
                                                       ,'CMP2_HOO_GLFUNC'
                                                       ,'CMP3_HOO_GLFUNC'
                                                       ,'BASE_RND'
                                                       ,'CMP1_RND'
                                                       ,'CMP2_RND'
                                                       ,'CMP3_RND'
                                                       ,'BASE_OIE'
                                                       ,'CMP1_OIE'
                                                       ,'CMP2_OIE'
                                                       ,'CMP3_OIE'
                                                       ) DEFAULT ON NULL (0)
                       )
    )
    SELECT   SCE_ELM_KEY
            ,CBU_COD
            ,SCE_ELM_COD
            ,DTA_YEA_COD
            ,PER_ELM_COD
            ,ACT_PER_FLG
            ,ETI_ELM_COD
            ,CUS_ELM_COD
            ,PDT_ELM_COD
            ,CAT_TYP_ELM_COD
            ,EIB_ELM_COD
            ,TTY_ELM_COD
            ,SAL_SUP_ELM_COD
            ,CUR_COD
            ,CNV_RAT_VAL
--            ,vol_unt_cod

            ,base_vol
            ,incr_vol
            ,cmp1_vol
            ,cmp2_vol
            ,cmp3_vol
            ,base_ns
            ,incr_ns
            ,cmp1_ns
            ,cmp2_ns
            ,cmp3_ns
            ,base_mat_cos
            ,incr_mat_cos
            ,cmp1_mat_cos
            ,cmp2_mat_cos
            ,cmp3_mat_cos
            ,base_mat_oth
            ,incr_mat_oth
            ,cmp1_mat_oth
            ,cmp2_mat_oth
            ,cmp3_mat_oth
            ,base_manuf_cos
            ,incr_manuf_cos
            ,cmp1_manuf_cos
            ,cmp2_manuf_cos
            ,cmp3_manuf_cos
            ,base_manuf_oth
            ,incr_manuf_oth
            ,cmp1_manuf_oth
            ,cmp2_manuf_oth
            ,cmp3_manuf_oth
            ,base_log_ftc_ifo
            ,incr_log_ftc_ifo
            ,cmp1_log_ftc_ifo
            ,cmp2_log_ftc_ifo
            ,cmp3_log_ftc_ifo
            ,base_log_usl
            ,incr_log_usl
            ,cmp1_log_usl
            ,cmp2_log_usl
            ,cmp3_log_usl
            ,base_log_oth
            ,incr_log_oth
            ,cmp1_log_oth
            ,cmp2_log_oth
            ,cmp3_log_oth
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
            ,base_oie
            ,incr_oie
            ,cmp1_oie
            ,cmp2_oie
            ,cmp3_oie
            ,fca_mat_oth
            ,fca_manuf_oth
            ,fca_log_oth
            ,var_ns
            ,var_mat_cos
            ,var_mat_oth
            ,var_manuf_cos
            ,var_manuf_oth
            ,var_log_ftc_ifo
            ,var_log_usl
            ,var_log_oth

            ,t_rec_ins_tst
            ,t_rec_upd_tst
    FROM     all_;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- Call the procedure to log the end of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

    -- 7. Return text when the stored procedure finish in success
    RETURN 'Success';

EXCEPTION
    WHEN OTHER THEN

        v_ERR_MSG := REPLACE(SQLCODE || ': ' || SQLERRM,'''','"');

        -- Assign the end date of the step
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

        -- Call the procedure to log the step in error with the error message
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, -1, :v_ERR_MSG, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         v_ERR_MSG := v_ERR_MSG || ' in the step ' || v_STEP_TABLE;

        v_STEP_TABLE := 'UPDATE STATUS TO failed IN R_FLX_SCE';
        v_STEP_NUM   := v_STEP_NUM + 1;
        
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

        -- xx. Update all scenarios to failed in case of failure
        UPDATE COP_DMT_FLX.R_FLX_SCE
        SET    INI_STS_COD = 'failed'
        WHERE  INI_STS_COD NOT IN ('created','done','failed','requested');

        -- Assign the end date of the step
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

        -- Call the procedure to log the step
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

        -- Call the procedure to log the end of the process
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);
        
        RETURN v_ERR_MSG;
        RAISE; -- Raise the same exception that you are handling.

END;
$$
;
