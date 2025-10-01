USE SCHEMA COP_DMT_FLX;


CREATE OR REPLACE PROCEDURE SP_Flex_Ini_Add_FCA_Var()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to Initialize the scenario for Flex (1st step)
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 20-11-2024                                                 
=========================================================================
Modified On:     Author:          Description: 

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

BEGIN

    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Ini_Add_FCA_Var','#','DLT',CURRENT_USER);

    -- 1. Truncate working table W_FLX_SRC_SCE__FCA_VAR
    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'Truncate working table W_FLX_SRC_SCE__FCA_VAR';
    v_STEP_NUM := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SRC_SCE__FCA_VAR;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 2. Retrieve missing Source scenarios data at lowest level, will be used for disaggregation

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'INSERT INTO W_FLX_SRC_SCE__FCA_VAR';
    v_STEP_NUM := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    /* insert to modify to load the FCA/Variability */
    
    INSERT INTO COP_DMT_FLX.W_FLX_SRC_SCE__FCA_VAR
               (CBU_COD
               ,SCE_ELM_COD
               ,CFG_ORD_NUM
               ,SRC_SCE_ELM_COD
               ,IND_GRP_COD
               ,IND_GRP_RAT_FLG
               ,BGN_PER_ELM_COD
               ,END_PER_ELM_COD
               ,ETI_GRP_COD
               ,CUS_GRP_COD
               ,CUS_DIM_GRP_COD
               ,PDT_GRP_COD
               ,CAT_TYP_GRP_COD
               ,EIB_GRP_COD
               ,TTY_GRP_COD
               ,SAL_SUP_GRP_COD
               ,IND_ELM_COD
               ,CFG_MOD_FLG
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
               ,RTO_DEN_AMT
               ,BAS_DEN_AMT
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
    SELECT      CBU_COD
               ,SCE_ELM_COD
               ,CFG_ORD_NUM
               ,SRC_SCE_ELM_COD
               ,IND_GRP_COD
               ,IND_GRP_RAT_FLG
               ,BGN_PER_ELM_COD
               ,END_PER_ELM_COD
               ,ETI_GRP_COD
               ,CUS_GRP_COD
               ,CUS_DIM_GRP_COD
               ,PDT_GRP_COD
               ,CAT_TYP_GRP_COD
               ,EIB_GRP_COD
               ,TTY_GRP_COD
               ,SAL_SUP_GRP_COD
               ,IND_ELM_COD
               ,CFG_MOD_FLG
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
               ,RTO_DEN_AMT
               ,BAS_DEN_AMT
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
               ,TO_TIMESTAMP(CURRENT_TIMESTAMP)    T_REC_UPD_TST
    FROM        COP_DMT_FLX.W_FLX_SRC_SCE__CLC;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- Call the procedure to log the end of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

    -- 4. Return text when the stored procedure finish in success
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

         -- xx. Update all scenarios init_in_progress to copy_failed in case of failure
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
