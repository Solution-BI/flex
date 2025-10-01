USE DATABASE {{env}}_COP;
USE SCHEMA COP_DMT_FLX;

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Populating F_FLX_SCE_INI Table

Author      : Noel Coquio (Solution BI France)
Created On  : 11-07-2024
=========================================================================
Modified On:    Description:                        Author:
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Scenario_Initialization()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in F_FLX_SCE_INI
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 01-07-2024                                                 
=========================================================================
Modified On: 29-07-2024    Description: - Remove Truncate -> Delete Insert if INI_STS_COD change to configured     Author: MOHAMMEDI Yanis
                                        - Add condition that stop SP if no scenario is configured        
Modified On: 08-11-2024    Description: - Review status
                                        - review Exception message       
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);
V_RUN_ID         VARCHAR(256);
v_STEP_NUM       NUMBER(5,0);
V_STEP_BEG_DT    VARCHAR(50);
V_STEP_END_DT    VARCHAR(50);
v_ERR_MSG        VARCHAR(1000);
v_IS_SCENARIO    INTEGER;

v_STS_PROC       VARCHAR(5000);
v_ERR_STEP       NUMBER(2,0);

v_cur_R_FLX_SCE CURSOR FOR SELECT CBU_COD,SCE_ELM_COD FROM COP_DMT_FLX.R_FLX_SCE WHERE INI_STS_COD = 'in_progress:3';
v_cur_F_FLX_CMP_SCE CURSOR FOR SELECT COUNT(*) FROM COP_DMT_FLX.F_FLX_CMP_SCE WHERE CBU_COD = ? AND SCE_ELM_COD = ?;
v_cur_W_FLX_SRC_SCE_FLT CURSOR FOR SELECT COUNT(*) FROM COP_DMT_FLX.W_FLX_SRC_SCE_FLT WHERE CBU_COD = ? AND SCE_ELM_COD = ?;

v_NB_ROW         INTEGER;
v_CBU_CODE       VARCHAR(10);
v_SCE_ELM_COD    VARCHAR(50);
BEGIN

   -- Generate the UUID for the procedure
   CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

   -- Call the procedure to log the init of the process
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Scenario_Initialization','#','DLT',CURRENT_USER);

   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'CHECK DATA TO TRANSFER';
   v_STEP_NUM   := 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   -- 1. Check if there is scenario requested

   SELECT Count(*)
   INTO   v_IS_SCENARIO
   FROM   COP_DMT_FLX.R_FLX_SCE
   WHERE  INI_STS_COD = 'requested';

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   IF (v_IS_SCENARIO = 0) THEN
      -- Call the procedure to log the end of the process
      CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

      RETURN 'Aucun Scénario configuré à initialiser actuellement...';
   END IF;

   -- 2. Call the procedure SP_Flex_Scenario_Ini_Copy_Default_Scenario: status requested to in_progress:1

   v_STEP_TABLE := 'SP_Flex_Scenario_Ini_Copy_Default_Scenario';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   CALL SP_Flex_Scenario_Ini_Copy_Default_Scenario();

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- Check if there is data in the working Table

   SELECT Count(*)
   INTO   v_NB_ROW
   FROM   COP_DMT_FLX.W_FLX_SRC_SCE;

   IF ( v_NB_ROW = 0 ) THEN
      -- xx. Update all scenarios in_progress:1 to no_data in case of no data
      UPDATE COP_DMT_FLX.R_FLX_SCE
      SET    INI_STS_COD = 'no_data_SRC_SCE'
      WHERE  INI_STS_COD = 'in_progress:1';

      -- Call the procedure to log the end of the process
      CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

      RETURN 'No data in the w_flx_src_sce';
   END IF;


   -- 3. Call the procedure SP_Flex_Scenario_Ini_Copy_Comparable_Scenario: status in_progress:1 to in_progress:2

   v_STEP_TABLE := 'SP_Flex_Scenario_Ini_Copy_Comparable_Scenario';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   CALL SP_Flex_Scenario_Ini_Copy_Comparable_Scenario();

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- 4. Call the procedure SP_Flex_Scenario_Ini_Source_Scenario_Filter: status in_progress:2 to in_progress:3

   v_STEP_TABLE := 'SP_Flex_Scenario_Ini_Source_Scenario_Filter';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   CALL SP_Flex_Scenario_Ini_Source_Scenario_Filter();

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- Check if there is data in the working Table
/*
   OPEN v_cur_R_FLX_SCE;
   FOR row_R_FLX_SCE IN v_cur_R_FLX_SCE LOOP
      v_CBU_CODE    := row_R_FLX_SCE.CBU_COD;
      v_SCE_ELM_COD := row_R_FLX_SCE.SCE_ELM_COD;
      OPEN v_cur_W_FLX_SRC_SCE_FLT USING (:v_CBU_CODE , :v_SCE_ELM_COD);
      FETCH v_cur_W_FLX_SRC_SCE_FLT INTO :v_NB_ROW;
      IF ( v_NB_ROW = 0 ) THEN 
         -- xx. Update all scenarios to no_data_SRC_SCE in case of no data
         UPDATE COP_DMT_FLX.R_FLX_SCE
         SET    INI_STS_COD = 'no_data_SRC_SCE'
         WHERE  CBU_COD     = :v_CBU_CODE
         AND    SCE_ELM_COD = :v_SCE_ELM_COD;
      END IF;
      CLOSE v_cur_W_FLX_SRC_SCE_FLT;
   END LOOP;
   CLOSE v_cur_R_FLX_SCE;
*/
   SELECT Count(*)
   INTO   v_NB_ROW
   FROM   COP_DMT_FLX.R_FLX_SCE
   WHERE  INI_STS_COD = 'in_progress:3';

   IF (v_NB_ROW > 0 ) THEN 
      -- 5. Call the procedure SP_Flex_Scenario_Ini_Ratio_Calculation: : status in_progress:3 to in_progress:4

      v_STEP_TABLE := 'SP_Flex_Scenario_Ini_Ratio_Calculation';
      v_STEP_NUM   := v_STEP_NUM + 1;

      SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

      CALL SP_Flex_Scenario_Ini_Ratio_Calculation();

      -- Assign the end date of the step
      SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

      -- Call the procedure to log the step
      CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

      -- Assign the step name, step num and start date of the step
      v_STEP_TABLE := 'UPDATE R_FLX_SCE FROM in_progress:4 TO in_progress:5';
      v_STEP_NUM := 1;

      SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

      UPDATE COP_DMT_FLX.R_FLX_SCE
      SET    INI_STS_COD = 'in_progress:5'
      WHERE  INI_STS_COD = 'in_progress:4';

      -- Assign the end date of the step
      SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

      -- Call the procedure to log the step
      CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

      -- 6. Delete the table F_FLX_SCE_INI
      v_STEP_TABLE := 'DELETE SCENARIO FROM F_FLX_SCE_INI';
      v_STEP_NUM   := v_STEP_NUM + 1;

      SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

      DELETE FROM COP_DMT_FLX.F_FLX_SCE_INI 
      WHERE SCE_ELM_KEY IN (SELECT SCE_ELM_KEY
                            FROM   COP_DMT_FLX.R_FLX_SCE
                            WHERE  INI_STS_COD = 'in_progress:5'
                           );

      -- Assign the end date of the step
      SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

      -- Call the procedure to log the step
      CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

      -- 7. Load the data in the table F_FLX_SCE_INI

      v_STEP_TABLE := 'INSERT F_FLX_SCE_INI';
      v_STEP_NUM   := v_STEP_NUM + 1;

      SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

      INSERT INTO COP_DMT_FLX.F_FLX_SCE_INI
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
                 ,vol_unt_cod
                 ,base_vol
                 ,incr_vol
                 ,cmp1_vol
                 ,cmp2_vol
                 ,cmp3_vol
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
                 ,conf_mat_oth_fca
                 ,incr_fca
                 ,base_ap
                 ,incr_ap
                 ,cmp1_ap
                 ,cmp2_ap
                 ,cmp3_ap
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
                 ,base_oie
                 ,incr_oie
                 ,cmp1_oie
                 ,cmp2_oie
                 ,cmp3_oie
                 ,pct_ns
                 ,pct_fcogs
                 ,pct_vcogs
                 ,t_rec_ins_tst
                 ,t_rec_upd_tst
                 )
--------------------------------------------------------------------------------
      with unpivot_source_scenarios as (
          /* Retreive all the comparable scenarios */
          SELECT   CBU_COD
                  ,SCE_ELM_COD
                  ,(CASE COLNAME
                         WHEN 'CMP_1ST_SRC_SCE_COD' THEN 'CMP1'
                         WHEN 'CMP_2ND_SRC_SCE_COD' THEN 'CMP2'
                         WHEN 'CMP_3RD_SRC_SCE_COD' THEN 'CMP3'
                    END)                                           AS SRC_SCE_TYP_COD
                  ,'F_FLX_CMP_SCE'                                 AS SCE_SRC_TAB_COD
                  ,DECODE(SPLIT_PART(SRC_SCE_ELM_COD,'-',2)
                         ,'',SRC_SCE_ELM_COD
                         ,SPLIT_PART(SRC_SCE_ELM_COD,'-',2))       AS SRC_SCE_ELM_COD
          FROM     COP_DMT_FLX.R_FLX_SCE
                   UNPIVOT INCLUDE NULLS (SRC_SCE_ELM_COD FOR COLNAME IN (CMP_1ST_SRC_SCE_COD
                                                                         ,CMP_2ND_SRC_SCE_COD
                                                                         ,CMP_3RD_SRC_SCE_COD))
          WHERE    R_FLX_SCE.T_REC_DLT_FLG = 0
          UNION ALL
          /* Retreive all the base scenarios included the scenario configured by KPI*/
          SELECT   DISTINCT 
                   R_FLX_SCE.CBU_COD
                  ,R_FLX_SCE.SCE_ELM_COD
                  ,'BASE'                                          AS SRC_SCE_TYP_COD
                  ,'F_FLX_SRC_SCE'                                 AS SCE_SRC_TAB_COD
                  ,P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD               AS SRC_SCE_ELM_COD
          FROM     COP_DMT_FLX.R_FLX_SCE
                   LEFT OUTER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON
                   (
                      R_FLX_SCE.SCE_ELM_KEY = P_FLX_SCE_CFG_IND.SCE_ELM_KEY
                   )
          WHERE    R_FLX_SCE.T_REC_DLT_FLG = 0
      )    
--------------------------------------------------------------------------------
-- Retreive all the currency rate by entity in the Source Scenario
      ,LIST_CUR_RAT AS (
          SELECT   sce.CBU_COD
                  ,sce.SCE_ELM_COD
                  ,src_data.ETI_ELM_COD
                  ,src_data.CNV_CUR_RAT
                  ,COUNT(*) nb_rows
          FROM     COP_DMT_FLX.R_FLX_SCE as sce
                   INNER JOIN unpivot_source_scenarios as src_sce ON 
                   (
                      sce.CBU_COD             = src_sce.CBU_COD     AND
                      sce.SCE_ELM_COD         = src_sce.SCE_ELM_COD AND
                      src_sce.SRC_SCE_TYP_COD = 'BASE'
                   )
                   INNER JOIN COP_DMT_FLX.V_ETL_F_FLX_SRC_CMP_SCE as src_data ON
                   (
                      src_sce.CBU_COD          = src_data.CBU_COD         AND
                      src_sce.SCE_ELM_COD      = src_data.SCE_ELM_COD     AND
                      src_sce.SRC_SCE_ELM_COD  = src_data.SRC_SCE_ELM_COD
                   )
          WHERE    src_data.CNV_CUR_RAT    IS NOT NULL
          AND      sce.ini_sts_cod         IN ('in_progress:5')
          GROUP BY sce.CBU_COD
                  ,sce.SCE_ELM_COD
                  ,src_data.ETI_ELM_COD
                  ,src_data.CNV_CUR_RAT
      )
      -- Define the rank of the currency using the number of rows
      ,RANK_CUR_RAT AS (
          SELECT   CBU_COD
                  ,SCE_ELM_COD
                  ,ETI_ELM_COD
                  ,CNV_CUR_RAT
                  ,RANK() OVER (PARTITION BY CBU_COD,SCE_ELM_COD,ETI_ELM_COD
                                ORDER BY nb_rows DESC) rnk_
          FROM     LIST_CUR_RAT
      )
      -- Retreive the FCA stored in Local Currency with the currency rate of the source scenario
      , SCE_ETI_PDT_FCA as (
          SELECT   RANK_CUR_RAT.CBU_COD,
                   RANK_CUR_RAT.SCE_ELM_COD
                  ,RANK_CUR_RAT.ETI_ELM_COD
                  ,PDT_GRP_COD
                  ,FCA_MAT_OTH_VAL
                  ,RANK_CUR_RAT.CNV_CUR_RAT
          FROM     RANK_CUR_RAT
                   LEFT OUTER JOIN COP_DMT_FLX.P_FLX_ETI_PDT_FCA as PDT_FCA ON
                   (
                      RANK_CUR_RAT.CBU_COD             = PDT_FCA.CBU_COD     AND
                      RANK_CUR_RAT.ETI_ELM_COD         = PDT_FCA.ETI_ELM_COD
                   )
          WHERE    RANK_CUR_RAT.rnk_ = 1
   )
   --------------------------------------------------------------------------------
      , agg_source_data as (

          SELECT   sce.SCE_ELM_KEY
                  ,sce.CBU_COD
                  ,sce.SCE_ELM_COD
                  ,sce.DTA_YEA_COD
                  ,src_data.PER_ELM_COD                                                      as per_elm_cod
                  ,ANY_VALUE(IFF(src_data.PER_ELM_COD <= sce.LST_ACT_PER_COD, 1, 0))         as act_per_flg
                  ,src_data.ETI_ELM_COD                                                      as eti_elm_cod
                  ,cus.CUS_GRP_COD                                                           as cus_elm_cod
                  ,src_data.PDT_ELM_COD                                                      as pdt_elm_cod
                  ,src_data.CAT_TYP_ELM_COD                                                  as cat_typ_elm_cod
                  ,src_data.EIB_ELM_COD                                                      as eib_elm_cod
                  ,src_data.TTY_ELM_COD                                                      as tty_elm_cod
                  ,src_data.SAL_SUP_ELM_COD                                                  as sal_sup_elm_cod
                  ,sce.CUR_COD
                  ,sce.NET_SALES_VAR_PCT
                  ,sce.FIXED_COGS_VAR_PCT
                  ,sce.VARIABLE_COGS_VAR_PCT
                  ,(CASE WHEN PDT_FCA.CNV_CUR_RAT IS NULL THEN 0
                         ELSE COALESCE(PDT_FCA.FCA_MAT_OTH_VAL,0) / PDT_FCA.CNV_CUR_RAT
                    END)                                                                     AS FCA_MAT_OTH_VAL
                  ,CAST(src_sce.SRC_SCE_TYP_COD ||'_'|| 
                        src_data.IND_ELM_COD AS VARCHAR(50))                                 as pvt_col_cod
                  ,SUM(src_data.AMOUNT)                                                      as amount
                  ,src_data.SCE_SRC_TAB_COD
                  -- Store applied rate in scenario
                  --,any_value(src_data.rate_cy) as rate_cy
                  --,any_value(src_data.rate_fy) as rate_fy
                  --,max(case when src_sce.src_sce_typ_cod = 'BASE' then src_data.t_rec_src_tst else null end) as t_base_src_tst
                  --,max(case when src_sce.src_sce_typ_cod = 'CMP1' then src_data.t_rec_src_tst else null end) as t_cmp1_src_tst
                  --,max(case when src_sce.src_sce_typ_cod = 'CMP2' then src_data.t_rec_src_tst else null end) as t_cmp2_src_tst
                  --,max(case when src_sce.src_sce_typ_cod = 'CMP3' then src_data.t_rec_src_tst else null end) as t_cmp3_src_tst

          FROM     COP_DMT_FLX.R_FLX_SCE as sce
--                   INNER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND cfg ON
--                   (
--                      sce.SCE_ELM_KEY = cfg.SCE_ELM_KEY
--                   )
                   INNER JOIN unpivot_source_scenarios as src_sce ON 
                   (
                      sce.CBU_COD      = src_sce.CBU_COD     AND
                      sce.SCE_ELM_COD  = src_sce.SCE_ELM_COD
                   )
                   INNER JOIN COP_DMT_FLX.V_ETL_F_FLX_SRC_CMP_SCE as src_data ON 
                   (
                      src_sce.CBU_COD          = src_data.CBU_COD         AND
                      src_sce.SCE_ELM_COD      = src_data.SCE_ELM_COD     AND
                      src_sce.SRC_SCE_ELM_COD  = src_data.SRC_SCE_ELM_COD AND
                      src_sce.SCE_SRC_TAB_COD  = src_data.SCE_SRC_TAB_COD
                   )
                   INNER JOIN COP_DMT_FLX.R_FLX_GRP_CUS as cus ON 
                   (
                      src_data.CBU_COD      = cus.CBU_COD         AND
                      src_data.CUS_ELM_COD  = cus.cus_elm_cod     AND 
                      sce.cus_dim_grp_cod   = cus.cus_dim_grp_cod
                   )
                   LEFT OUTER JOIN COP_DMT_FLX.R_FLX_PDT pdt ON
                   (
                     src_data.CBU_COD      = pdt.CBU_COD     AND
                     src_data.PDT_ELM_COD  = pdt.PDT_ELM_COD
                   )
                   LEFT OUTER JOIN sce_ETI_PDT_FCA as PDT_FCA ON
                   (
                      src_data.CBU_COD         = PDT_FCA.CBU_COD     AND
                      src_data.ETI_ELM_COD     = PDT_FCA.ETI_ELM_COD AND
                      sce.SCE_ELM_COD          = PDT_FCA.SCE_ELM_COD AND
                      PDT_FCA.PDT_GRP_COD      = pdt.LV0_PDT_CAT_COD AND
                      src_sce.SRC_SCE_TYP_COD  = 'BASE'
                   )
          WHERE    1 = 1
          AND      sce.ini_sts_cod           IN ('in_progress:5')
          AND      src_data.IND_ELM_COD      <> 'NA'
          GROUP BY ALL

      )
      , all_ as (
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
              ,1                                                                      as CNV_RAT_VAL
              -- Add into Entity configuration
              ,(CASE WHEN CBU_COD = 'IBE' THEN 
                          (case WHEN ETI_ELM_COD IN ('0036', '0051') THEN 'TONS'
                                WHEN ETI_ELM_COD IN ('0052')         THEN 'KLITERS'
                                ELSE 'DRY'
                           END)
                     ELSE 'DRY' 
                END)                                                                   as vol_unt_cod

              ,(CASE WHEN CBU_COD = 'IBE' THEN 
                          (case WHEN ETI_ELM_COD IN ('0036', '0051') THEN "'BASE_VOL_TONS'"
                                WHEN ETI_ELM_COD IN ('0052')         THEN "'BASE_VOL_KLITERS'"
                                ELSE "'BASE_VOL_DRY'"
                           END)
                     ELSE "'BASE_VOL_DRY'"
                END)                                                                   as base_vol
              ,0                                                                       as incr_vol
              ,(CASE WHEN CBU_COD = 'IBE' THEN 
                          (case WHEN ETI_ELM_COD IN ('0036', '0051') THEN "'CMP1_VOL_TONS'"
                                WHEN ETI_ELM_COD IN ('0052')         THEN "'CMP1_VOL_KLITERS'"
                                ELSE "'CMP1_VOL_DRY'"
                           END)
                     ELSE "'CMP1_VOL_DRY'"
                END)                                                                   as cmp1_vol
              ,(CASE WHEN CBU_COD = 'IBE' THEN 
                          (case WHEN ETI_ELM_COD IN ('0036', '0051') THEN "'CMP2_VOL_TONS'"
                                WHEN ETI_ELM_COD IN ('0052')         THEN "'CMP2_VOL_KLITERS'"
                                ELSE "'CMP2_VOL_DRY'"
                           END)
                     ELSE "'CMP2_VOL_DRY'" 
                END)                                                                   as cmp2_vol
              ,(CASE WHEN CBU_COD = 'IBE' THEN 
                          (case WHEN ETI_ELM_COD IN ('0036', '0051') THEN "'CMP3_VOL_TONS'"
                                WHEN ETI_ELM_COD IN ('0052')         THEN "'CMP3_VOL_KLITERS'"
                                ELSE "'CMP3_VOL_DRY'"
                           END)
                     ELSE "'CMP3_VOL_DRY'" 
                END)                                                                   as cmp3_vol
              ,"'BASE_GS'"                                                             as base_gs
              ,0                                                                       as incr_gs
              ,"'CMP1_GS'"                                                             as cmp1_gs
              ,"'CMP2_GS'"                                                             as cmp2_gs
              ,"'CMP3_GS'"                                                             as cmp3_gs

              ,"'BASE_SD'"                                                             as base_sd
              ,0                                                                       as incr_sd
              ,"'CMP1_SD'"                                                             as cmp1_sd
              ,"'CMP2_SD'"                                                             as cmp2_sd
              ,"'CMP3_SD'"                                                             as cmp3_sd

              ,"'BASE_DR'"                                                             as base_dr
              ,0                                                                       as incr_dr
              ,"'CMP1_DR'"                                                             as cmp1_dr
              ,"'CMP2_DR'"                                                             as cmp2_dr
              ,"'CMP3_DR'"                                                             as cmp3_dr

              ,"'BASE_TS'"                                                             as base_ts
              ,0                                                                       as incr_ts
              ,"'CMP1_TS'"                                                             as cmp1_ts
              ,"'CMP2_TS'"                                                             as cmp2_ts
              ,"'CMP3_TS'"                                                             as cmp3_ts

              ,"'BASE_MAT_FIX'"                                                        as base_mat_fix
              ,0                                                                       as incr_mat_fix
              ,"'CMP1_MAT_FIX'"                                                        as cmp1_mat_fix
              ,"'CMP2_MAT_FIX'"                                                        as cmp2_mat_fix
              ,"'CMP3_MAT_FIX'"                                                        as cmp3_mat_fix

              ,"'BASE_MAT_VAR'"                                                        as base_mat_var
              ,0                                                                       as incr_mat_var
              ,"'CMP1_MAT_VAR'"                                                        as cmp1_mat_var
              ,"'CMP2_MAT_VAR'"                                                        as cmp2_mat_var
              ,"'CMP3_MAT_VAR'"                                                        as cmp3_mat_var

              ,"'BASE_MANUF_FIX'"                                                      as base_manuf_fix
              ,0                                                                       as incr_manuf_fix
              ,"'CMP1_MANUF_FIX'"                                                      as cmp1_manuf_fix
              ,"'CMP2_MANUF_FIX'"                                                      as cmp2_manuf_fix
              ,"'CMP3_MANUF_FIX'"                                                      as cmp3_manuf_fix

              ,"'BASE_MANUF_VAR'"                                                      as base_manuf_var
              ,0                                                                       as incr_manuf_var
              ,"'CMP1_MANUF_VAR'"                                                      as cmp1_manuf_var
              ,"'CMP2_MANUF_VAR'"                                                      as cmp2_manuf_var
              ,"'CMP3_MANUF_VAR'"                                                      as cmp3_manuf_var

              ,"'BASE_LOG_FIX'"                                                        as base_log_fix
              ,0                                                                       as incr_log_fix
              ,"'CMP1_LOG_FIX'"                                                        as cmp1_log_fix
              ,"'CMP2_LOG_FIX'"                                                        as cmp2_log_fix
              ,"'CMP3_LOG_FIX'"                                                        as cmp3_log_fix

              ,"'BASE_LOG_VAR'"                                                        as base_log_var
              ,0                                                                       as incr_log_var
              ,"'CMP1_LOG_VAR'"                                                        as cmp1_log_var
              ,"'CMP2_LOG_VAR'"                                                        as cmp2_log_var
              ,"'CMP3_LOG_VAR'"                                                        as cmp3_log_var

              ,FCA_MAT_OTH_VAL                                                         as conf_mat_oth_fca
              ,0                                                                       as incr_fca

              ,"'BASE_AP'"                                                             as base_ap
              ,0                                                                       as incr_ap
              ,"'CMP1_AP'"                                                             as cmp1_ap
              ,"'CMP2_AP'"                                                             as cmp2_ap
              ,"'CMP3_AP'"                                                             as cmp3_ap

              ,"'BASE_SFO'"                                                            as base_sfo
              ,0                                                                       as incr_sfo
              ,"'CMP1_SFO'"                                                            as cmp1_sfo
              ,"'CMP2_SFO'"                                                            as cmp2_sfo
              ,"'CMP3_SFO'"                                                            as cmp3_sfo

              ,"'BASE_HOO'"                                                            as base_hoo
              ,0                                                                       as incr_hoo
              ,"'CMP1_HOO'"                                                            as cmp1_hoo
              ,"'CMP2_HOO'"                                                            as cmp2_hoo
              ,"'CMP3_HOO'"                                                            as cmp3_hoo

              ,"'BASE_RNI'"                                                            as base_rni
              ,0                                                                       as incr_rni
              ,"'CMP1_RNI'"                                                            as cmp1_rni
              ,"'CMP2_RNI'"                                                            as cmp2_rni
              ,"'CMP3_RNI'"                                                            as cmp3_rni

              ,"'BASE_OIE'"                                                            as base_oie
              ,0                                                                       as incr_oie
              ,"'CMP1_OIE'"                                                            as cmp1_oie
              ,"'CMP2_OIE'"                                                            as cmp2_oie
              ,"'CMP3_OIE'"                                                            as cmp3_oie

              ,NET_SALES_VAR_PCT                                                       as pct_ns
              ,FIXED_COGS_VAR_PCT                                                      as pct_fcogs
              ,VARIABLE_COGS_VAR_PCT                                                   as pct_vcogs

              ,current_timestamp                                                       as t_rec_ins_tst
              ,current_timestamp                                                       as t_rec_upd_tst

      from     agg_source_data pivot (sum(amount)
               /* -- Using this query does not keep the order from the `order by` clause
                   select (ind_type.value::varchar ||'_'|| ind.ind_elm_cod)::varchar(50)
                       , (''''||ind_type.value::varchar ||'_'|| ind.ind_elm_cod||''',')::varchar(50) as pvt_col_cod
                       , ('"'''||ind_type.value::varchar ||'_'|| ind.ind_elm_cod||'''" as '||lower(ind_type.value::varchar ||'_'|| ind.ind_elm_cod)||',')::varchar(50) as pvt_col_cod
                   from cop_dmt_flx.r_flx_ind as ind,
                       table(flatten(['BASE', 'CMP1', 'CMP2', 'CMP3'])) as ind_type
                   where ind_fml_txt = ''
                       and ind_elm_cod not in ('VOL')  -- Calculated, missing specific formula
                   order by ind.ind_ord_num, ind_type.index
               */

                                      for pvt_col_cod in ('BASE_VOL_UNITS'
                                                         ,'CMP1_VOL_UNITS'
                                                         ,'CMP2_VOL_UNITS'
                                                         ,'CMP3_VOL_UNITS'
                                                         ,'BASE_VOL_TONS'
                                                         ,'CMP1_VOL_TONS'
                                                         ,'CMP2_VOL_TONS'
                                                         ,'CMP3_VOL_TONS'
                                                         ,'BASE_VOL_DRY'
                                                         ,'CMP1_VOL_DRY'
                                                         ,'CMP2_VOL_DRY'
                                                         ,'CMP3_VOL_DRY'
                                                         ,'BASE_VOL_KLITERS'
                                                         ,'CMP1_VOL_KLITERS'
                                                         ,'CMP2_VOL_KLITERS'
                                                         ,'CMP3_VOL_KLITERS'
                                                         ,'BASE_GS'
                                                         ,'CMP1_GS'
                                                         ,'CMP2_GS'
                                                         ,'CMP3_GS'
                                                         ,'BASE_SD'
                                                         ,'CMP1_SD'
                                                         ,'CMP2_SD'
                                                         ,'CMP3_SD'
                                                         ,'BASE_DR'
                                                         ,'CMP1_DR'
                                                         ,'CMP2_DR'
                                                         ,'CMP3_DR'
                                                         ,'BASE_TS'
                                                         ,'CMP1_TS'
                                                         ,'CMP2_TS'
                                                         ,'CMP3_TS'
                                                         ,'BASE_MAT_FIX'
                                                         ,'CMP1_MAT_FIX'
                                                         ,'CMP2_MAT_FIX'
                                                         ,'CMP3_MAT_FIX'
                                                         ,'BASE_MAT_VAR'
                                                         ,'CMP1_MAT_VAR'
                                                         ,'CMP2_MAT_VAR'
                                                         ,'CMP3_MAT_VAR'
                                                         ,'BASE_MANUF_FIX'
                                                         ,'CMP1_MANUF_FIX'
                                                         ,'CMP2_MANUF_FIX'
                                                         ,'CMP3_MANUF_FIX'
                                                         ,'BASE_MANUF_VAR'
                                                         ,'CMP1_MANUF_VAR'
                                                         ,'CMP2_MANUF_VAR'
                                                         ,'CMP3_MANUF_VAR'
                                                         ,'BASE_LOG_FIX'
                                                         ,'CMP1_LOG_FIX'
                                                         ,'CMP2_LOG_FIX'
                                                         ,'CMP3_LOG_FIX'
                                                         ,'BASE_LOG_VAR'
                                                         ,'CMP1_LOG_VAR'
                                                         ,'CMP2_LOG_VAR'
                                                         ,'CMP3_LOG_VAR'
                                                         --,'BASE_COGS_TOT_FCA'
                                                         --,'CMP1_COGS_TOT_FCA'
                                                         --,'CMP2_COGS_TOT_FCA'
                                                         --,'CMP3_COGS_TOT_FCA'
                                                         ,'BASE_AP'
                                                         ,'CMP1_AP'
                                                         ,'CMP2_AP'
                                                         ,'CMP3_AP'
                                                         ,'BASE_SFO'
                                                         ,'CMP1_SFO'
                                                         ,'CMP2_SFO'
                                                         ,'CMP3_SFO'
                                                         ,'BASE_HOO'
                                                         ,'CMP1_HOO'
                                                         ,'CMP2_HOO'
                                                         ,'CMP3_HOO'
                                                         ,'BASE_RNI'
                                                         ,'CMP1_RNI'
                                                         ,'CMP2_RNI'
                                                         ,'CMP3_RNI'
                                                         ,'BASE_OIE'
                                                         ,'CMP1_OIE'
                                                         ,'CMP2_OIE'
                                                         ,'CMP3_OIE'
                                                         ) default on null (0)
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
              ,vol_unt_cod

              ,base_vol
              ,incr_vol
              ,cmp1_vol
              ,cmp2_vol
              ,cmp3_vol

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

              ,base_mat_fix + base_mat_var base_mat
              ,incr_mat_fix + incr_mat_var incr_mat
              ,cmp1_mat_fix + cmp1_mat_var cmp1_mat
              ,cmp2_mat_fix + cmp2_mat_var cmp2_mat
              ,cmp3_mat_fix + cmp3_mat_var cmp3_mat

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

              ,base_manuf_fix + base_manuf_var base_manuf
              ,incr_manuf_fix + incr_manuf_var incr_manuf
              ,cmp1_manuf_fix + cmp1_manuf_var cmp1_manuf
              ,cmp2_manuf_fix + cmp2_manuf_var cmp2_manuf
              ,cmp3_manuf_fix + cmp3_manuf_var cmp3_manuf

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

              ,base_log_fix + base_log_var base_log
              ,incr_log_fix + incr_log_var incr_log
              ,cmp1_log_fix + cmp1_log_var cmp1_log
              ,cmp2_log_fix + cmp2_log_var cmp2_log
              ,cmp3_log_fix + cmp3_log_var cmp3_log

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

              ,conf_mat_oth_fca
              ,incr_fca

              ,base_ap
              ,incr_ap
              ,cmp1_ap
              ,cmp2_ap
              ,cmp3_ap

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

              ,base_oie
              ,incr_oie
              ,cmp1_oie
              ,cmp2_oie
              ,cmp3_oie

              ,pct_ns
              ,pct_fcogs
              ,pct_vcogs

              ,t_rec_ins_tst
              ,t_rec_upd_tst
      from     all_;
      -- Assign the end date of the step
      SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

      -- Call the procedure to log the step
      CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   END IF;

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

        v_STEP_TABLE := 'UPDATE STATUS TO failed IN R_FLX_SCE';
        v_STEP_NUM   := v_STEP_NUM + 1;
        
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

        -- xx. Update all scenarios init_in_progress to copy_failed in case of failure
        UPDATE COP_DMT_FLX.R_FLX_SCE
        SET    INI_STS_COD = 'failed'
        WHERE  INI_STS_COD LIKE 'in_progress%';

        -- Assign the end date of the step
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

        -- Call the procedure to log the step
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

        -- Call the procedure to log the end of the process
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);
        
        RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
        RAISE; -- Raise the same exception that you are handling.

END;
$$
;
