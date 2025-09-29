USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE PROCEDURE SP_Flex_Scenario_Ini_Source_Scenario_Filter()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in W_FLX_SRC_SCE_FLT
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 26-08-2024                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);
V_RUN_ID         VARCHAR(256);
v_STEP_NUM       NUMBER(5,0);
V_STEP_BEG_DT    VARCHAR(50);
V_STEP_END_DT    VARCHAR(50);
v_ERR_MSG        VARCHAR(1000);
BEGIN

   CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Scenario_Ini_Source_Scenario_Filter','#','DLT',CURRENT_USER);

   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'UPDATE R_FLX_SCE FROM in_progress:2 TO in_progress:3';
   v_STEP_NUM := 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   UPDATE COP_DMT_FLX.R_FLX_SCE
   SET    INI_STS_COD = 'in_progress:3'
   WHERE  INI_STS_COD = 'in_progress:2';

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- 1. Truncate the table W_FLX_SRC_SCE_FLT
   v_STEP_TABLE := 'TRUNCATE W_FLX_SRC_SCE_FLT';
   v_STEP_NUM := v_STEP_NUM + 1;
   
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z')
   INTO   :V_STEP_BEG_DT;
   
   TRUNCATE TABLE COP_DMT_FLX.W_FLX_SRC_SCE_FLT;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z')
   INTO   :V_STEP_END_DT;
   
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- 2. Insert 
   v_STEP_TABLE := 'INSERT W_FLX_SRC_SCE_FLT FROM V_ETL_W_FLX_SRC_SCE_FLT';
   v_STEP_NUM := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z')
   INTO   :V_STEP_BEG_DT;

   INSERT INTO COP_DMT_FLX.W_FLX_SRC_SCE_FLT
              (SCE_ELM_KEY
              ,CBU_COD
              ,SCE_ELM_COD
              ,CFG_ORD_NUM
              ,SRC_SCE_ELM_COD
              ,IND_GRP_COD
              ,IND_GRP_RAT_FLG
              ,BGN_PER_ELM_COD
              ,END_PER_ELM_COD
              ,ETI_GRP_COD
              ,CUS_GRP_COD
              ,PDT_GRP_COD
              ,CAT_TYP_GRP_COD
              ,EIB_GRP_COD
              ,TTY_GRP_COD
              ,SAL_SUP_GRP_COD
              ,IND_ELM_COD
              ,IND_NUM_FLG
              ,IND_DEN_FLG
              ,PER_ELM_COD
              ,ACT_PER_FLG
              ,ETI_ELM_COD
              ,CUS_ELM_COD
              ,PDT_ELM_COD
              ,CAT_TYP_ELM_COD
              ,EIB_ELM_COD
              ,TTY_ELM_COD
              ,SAL_SUP_ELM_COD
              ,SRC_CUR_COD
              ,SCE_CUR_COD
              ,CNV_CUR_RAT
              ,AMOUNT
              ,ACCOUNT_ELEMENT_CODE
              ,DESTINATION_ELEMENT_CODE
              ,FUNCTIONAL_AREA_ELEMENT_CODE
              ,CATEGORY_ELEMENT_CODE
              ,CHANNEL_ELEMENT_CODE
              ,IOM_CODE
              ,PLANT_CODE
              ,ORIGINAL_ACCOUNT_ELEMENT_CODE
              ,T_REC_SRC_TST
              ,T_REC_INS_TST
              ,T_REC_UPD_TST
              )
   SELECT    SCE_ELM_KEY
            ,CBU_COD
            ,SCE_ELM_COD
            ,CFG_ORD_NUM
            ,SRC_SCE_ELM_COD
            ,IND_GRP_COD
            ,IND_GRP_RAT_FLG
            ,BGN_PER_ELM_COD
            ,END_PER_ELM_COD
            ,ETI_GRP_COD
            ,CUS_GRP_COD
            ,PDT_GRP_COD
            ,CAT_TYP_GRP_COD
            ,EIB_GRP_COD
            ,TTY_GRP_COD
            ,SAL_SUP_GRP_COD
            ,IND_ELM_COD
            ,IND_NUM_FLG
            ,IND_DEN_FLG
            ,PER_ELM_COD
            ,ACT_PER_FLG
            ,ETI_ELM_COD
            ,CUS_ELM_COD
            ,PDT_ELM_COD
            ,CAT_TYP_ELM_COD
            ,EIB_ELM_COD
            ,TTY_ELM_COD
            ,SAL_SUP_ELM_COD
            ,SRC_CUR_COD
            ,SCE_CUR_COD
            ,CNV_CUR_RAT
            ,AMOUNT
            ,ACCOUNT_ELEMENT_CODE
            ,DESTINATION_ELEMENT_CODE
            ,FUNCTIONAL_AREA_ELEMENT_CODE
            ,CATEGORY_ELEMENT_CODE
            ,CHANNEL_ELEMENT_CODE
            ,IOM_CODE
            ,PLANT_CODE
            ,ORIGINAL_ACCOUNT_ELEMENT_CODE
            ,T_REC_SRC_TST
            ,T_REC_INS_TST
            ,T_REC_UPD_TST
   FROM      COP_DMT_FLX.V_ETL_W_FLX_SRC_SCE_FLT;


   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z')
   INTO   :V_STEP_END_DT;
   
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- 3. Log the flow in success
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

   EXCEPTION
   WHEN OTHER THEN

        v_ERR_MSG := REPLACE(SQLCODE || ': ' || SQLERRM,'''','"');

        -- Assign the end date of the step
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

        -- Call the procedure to log the step in error with the error message
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, -1, :v_ERR_MSG, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

        -- xx. Update all scenarios in_progress:3 to failed in case of failure

        v_STEP_TABLE := 'UPDATE STATUS TO failed IN R_FLX_SCE';
        v_STEP_NUM   := v_STEP_NUM + 1;
        
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

        UPDATE COP_DMT_FLX.R_FLX_SCE
        SET    INI_STS_COD = 'failed'
        WHERE  INI_STS_COD = 'in_progress:3';

        -- Assign the end date of the step
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

        -- Call the procedure to log the step
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

        -- Log the flow in error
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

        RETURN SQLCODE || ': ' || SQLERRM;
        RAISE; -- Raise the same exception that you are handling.

END;
$$
;