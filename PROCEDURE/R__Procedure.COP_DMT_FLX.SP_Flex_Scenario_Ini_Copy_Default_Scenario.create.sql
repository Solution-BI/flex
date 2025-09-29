USE DATABASE {{env}}_COP;
USE SCHEMA COP_DMT_FLX{{uid}};

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Copy Scneario From CC to w_flx_src_sce 

Author      : Yanis Mohammedi (Solution BI France)
Created On  : 23-08-2024
=========================================================================
Modified On:    Description:                        Author:
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Scenario_Ini_Copy_Default_Scenario()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
DECLARE
v_STEP_TABLE     VARCHAR(256);
V_RUN_ID         VARCHAR(256);
v_STEP_NUM       NUMBER(5,0);
V_STEP_BEG_DT    VARCHAR(50);
V_STEP_END_DT    VARCHAR(50);
v_ERR_MSG        VARCHAR(1000);
v_IS_SCENARIO    INTEGER;

BEGIN
    
   -- Generate the UUID for the procedure
   CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

   -- Call the procedure to log the init of the process
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Scenario_Ini_Copy_Default_Scenario','#','DLT',CURRENT_USER);

   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'UPDATE R_FLX_SCE FROM requested TO in_progress:1';
   v_STEP_NUM := 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   UPDATE COP_DMT_FLX.R_FLX_SCE
   SET    INI_STS_COD = 'in_progress:1'
   WHERE  INI_STS_COD = 'requested';

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- 2. Retrieve missing Source scenarios data from Controlling Cloud (copy at lowest level, will be used for disaggregation)

   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'TRUNCATE w_flx_src_sce';
   v_STEP_NUM := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   TRUNCATE TABLE w_flx_src_sce;

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);
   
    -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'INSERT Default Scenario INTO w_flx_src_sce';
   v_STEP_NUM := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   INSERT INTO w_flx_src_sce
         (CBU_COD
         ,SCENARIO_TYPE_CODE
         ,SCENARIO_ELEMENT_CODE
         ,RVN_DATE
         ,SCENARIO_DATE
         ,PERIOD_ELEMENT_CODE
         ,ENTITY_ELEMENT_CODE
         ,ACCOUNT_ELEMENT_CODE
         ,DESTINATION_ELEMENT_CODE
         ,FUNCTIONAL_AREA_ELEMENT_CODE
         ,SU_SP_SPLIT_CODE
         ,CUSTOMER_ELEMENT_CODE
         ,PRODUCT_ELEMENT_CODE
         --,INNO_RENO_KEY_CODE
         --,PRODUCT_ATTRIBUTE_KEY_CODE
         ,CATEGORY_ELEMENT_CODE
         ,MGR_L500_CODE
         ,EIB_ELEMENT_CODE
         ,TERRITORY_ELEMENT_CODE
         ,CHANNEL_ELEMENT_CODE
         ,IOM_CODE
         ,PLANT_CODE
         ,ORIGINAL_ACCOUNT_ELEMENT_CODE
         ,indicator_code
         ,RATE_CY
         ,RATE_FY
         ,AMOUNT_LC
         ,AMOUNT_EUR_CY
         ,AMOUNT_EUR_FY
         --,AMOUNT_EUR
         ,CUR_COD
         ,T_REC_SRC_TST
         ,T_REC_INS_TST
         ,T_REC_UPD_TST
         )
   WITH FLX_SCE as
      (
         SELECT DISTINCT P_FLX_SCE_CFG_IND.CBU_COD, P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD
         FROM   COP_DMT_FLX.R_FLX_SCE
                INNER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON 
                (
                  R_FLX_SCE.SCE_ELM_KEY = P_FLX_SCE_CFG_IND.SCE_ELM_KEY
                )
         WHERE R_FLX_SCE.INI_STS_COD = 'in_progress:1'
      )
   SELECT   cc.CBU_COD
           ,cc.SCENARIO_TYPE_CODE
           ,cc.SCENARIO_ELEMENT_CODE
           ,cc.RVN_DATE
           ,cc.SCENARIO_DATE
           ,cc.PERIOD_ELEMENT_CODE
           ,cc.ENTITY_ELEMENT_CODE
           ,cc.ACCOUNT_ELEMENT_CODE
           ,cc.DESTINATION_ELEMENT_CODE
           ,cc.FUNCTIONAL_AREA_ELEMENT_CODE
           ,cc.SU_SP_SPLIT_CODE
           ,cc.CUSTOMER_ELEMENT_CODE
           ,cc.PRODUCT_ELEMENT_CODE
           --,cc.INNO_RENO_KEY_CODE  -- Not used in Flex, will be recalculated from the other elements
           --,cc.PRODUCT_ATTRIBUTE_KEY_CODE  -- Not used in Flex, will be recalculated from the other elements
           ,cc.CATEGORY_ELEMENT_CODE
           ,cc.MGR_L500_CODE
           ,cc.EIB_ELEMENT_CODE
           ,cc.TERRITORY_ELEMENT_CODE
           ,CAST('NA' AS varchar(30))               as channel_element_code  -- Not used in Flex, writeback will be done using NA
           ,CAST('NA' AS varchar(30))               as iom_code  -- Not used in Flex, writeback will be done using NA
           ,CAST('NA' AS varchar(30))               as plant_code  -- Not used in Flex, writeback will be done using NA
           ,cc.ORIGINAL_ACCOUNT_ELEMENT_CODE
           ,cc.IND_ELM_COD                          as indicator_code
           ,cc.RATE_CY
           ,cc.RATE_FY
           ,cc.AMOUNT_LC
           ,cc.AMOUNT_EUR_CY
           ,cc.AMOUNT_EUR_FY
           --,cc.AMOUNT_EUR  -- Not used in Flex
           ,cc.CUR_COD
           ,cc.T_UPD_TST                            as t_rec_src_tst
           ,current_timestamp                       as t_rec_ins_tst
           ,current_timestamp                       as t_rec_upd_tst
   FROM     COP_DSP_CONTROLLING_CLOUD.F_CCD_FULL_DTL_PNL as cc
            INNER JOIN FLX_SCE ON
            (
               FLX_SCE.CBU_COD         = cc.CBU_COD               AND
               FLX_SCE.SRC_SCE_ELM_COD = cc.SCENARIO_ELEMENT_CODE
            )
   ;

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- Call the procedure to log the end of the process
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

   -- 3. Return text when the stored procedure finish in success
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
        WHERE  INI_STS_COD = 'in_progress:1';

        -- Assign the end date of the step
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

        -- Call the procedure to log the step
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

        -- Call the procedure to log the step
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

        
        RETURN SQLCODE || ': ' || SQLERRM;
        RAISE; -- Raise the same exception that you are handling.

END;

$$
;