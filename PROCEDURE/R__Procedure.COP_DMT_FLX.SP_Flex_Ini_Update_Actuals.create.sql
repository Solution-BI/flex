USE SCHEMA COP_DMT_FLX{{uid}};


CREATE OR REPLACE PROCEDURE SP_Flex_Ini_Update_Actuals()
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
Created On  : 27-11-2024                                                 
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
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Ini_Update_Actuals','#','DLT',CURRENT_USER);

    -- 1. Truncate working table 

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'Truncate working table W_FLX_SRC_SCE__ACT';
    v_STEP_NUM := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SRC_SCE__ACT;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 2. Load the Actual data and keep the source scenario for the missing entity & period

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'INSERT INTO W_FLX_SRC_SCE__ACT';
    v_STEP_NUM := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    INSERT INTO COP_DMT_FLX.W_FLX_SRC_SCE__ACT
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
    WITH Actual_ AS (
        SELECT   R_FLX_SCE.CBU_COD
                ,R_FLX_SCE.SCE_ELM_COD
                ,ETI_ELM_COD
                ,MAX(PER_ELM_COD)       PER_ELM_COD
        FROM     COP_DMT_FLX.R_FLX_SCE
                 INNER JOIN COP_DMT_FLX.W_FLX_CMP_SCE_FLT ON
                 (
                      W_FLX_CMP_SCE_FLT.CBU_COD         = R_FLX_SCE.CBU_COD         AND
                      W_FLX_CMP_SCE_FLT.SCE_ELM_COD     = R_FLX_SCE.SCE_ELM_COD     AND
                      W_FLX_CMP_SCE_FLT.SRC_SCE_ELM_COD = R_FLX_SCE.ACT_SRC_SCE_COD
                 )
        WHERE    R_FLX_SCE.INI_STS_COD NOT IN ('created','done','failed','requested')
        AND      R_FLX_SCE.UPD_ACT_FLG      = 1
        GROUP BY ALL
    )
   ,Src_Filter AS (
        SELECT   DISTINCT
                 W_FLX_SRC_SCE__FCA_VAR.CBU_COD
                ,W_FLX_SRC_SCE__FCA_VAR.SCE_ELM_COD
                ,W_FLX_SRC_SCE__FCA_VAR.ETI_ELM_COD
                ,COALESCE(Actual_.PER_ELM_COD,'00')                    PER_ELM_COD
        FROM     COP_DMT_FLX.W_FLX_SRC_SCE__FCA_VAR
                 LEFT OUTER JOIN Actual_ ON 
                 (
                      W_FLX_SRC_SCE__FCA_VAR.CBU_COD     = Actual_.CBU_COD     AND
                      W_FLX_SRC_SCE__FCA_VAR.SCE_ELM_COD = Actual_.SCE_ELM_COD AND
                      W_FLX_SRC_SCE__FCA_VAR.ETI_ELM_COD = Actual_.ETI_ELM_COD
                 )
    )
    SELECT      W_FLX_CMP_SCE_FLT.CBU_COD
               ,W_FLX_CMP_SCE_FLT.SCE_ELM_COD
               ,W_FLX_CMP_SCE_FLT.CFG_ORD_NUM
               ,W_FLX_CMP_SCE_FLT.SRC_SCE_ELM_COD
               ,W_FLX_CMP_SCE_FLT.IND_GRP_COD
               ,W_FLX_CMP_SCE_FLT.IND_GRP_RAT_FLG
               ,W_FLX_CMP_SCE_FLT.BGN_PER_ELM_COD
               ,W_FLX_CMP_SCE_FLT.END_PER_ELM_COD
               ,W_FLX_CMP_SCE_FLT.ETI_GRP_COD
               ,W_FLX_CMP_SCE_FLT.CUS_GRP_COD
               ,W_FLX_CMP_SCE_FLT.CUS_DIM_GRP_COD
               ,W_FLX_CMP_SCE_FLT.PDT_GRP_COD
               ,W_FLX_CMP_SCE_FLT.CAT_TYP_GRP_COD
               ,W_FLX_CMP_SCE_FLT.EIB_GRP_COD
               ,W_FLX_CMP_SCE_FLT.TTY_GRP_COD
               ,W_FLX_CMP_SCE_FLT.SAL_SUP_GRP_COD
               ,W_FLX_CMP_SCE_FLT.IND_ELM_COD
               ,W_FLX_CMP_SCE_FLT.CFG_MOD_FLG
               ,W_FLX_CMP_SCE_FLT.IND_NUM_FLG
               ,W_FLX_CMP_SCE_FLT.IND_DEN_FLG
               ,W_FLX_CMP_SCE_FLT.PER_ELM_COD
               ,W_FLX_CMP_SCE_FLT.ACT_PER_FLG
               ,W_FLX_CMP_SCE_FLT.ETI_ELM_COD
               ,W_FLX_CMP_SCE_FLT.CUS_ELM_COD
               ,W_FLX_CMP_SCE_FLT.PDT_ELM_COD
               ,W_FLX_CMP_SCE_FLT.CAT_TYP_ELM_COD
               ,W_FLX_CMP_SCE_FLT.EIB_ELM_COD
               ,W_FLX_CMP_SCE_FLT.TTY_ELM_COD
               ,W_FLX_CMP_SCE_FLT.SAL_SUP_ELM_COD
               ,W_FLX_CMP_SCE_FLT.SRC_CUR_COD
               ,W_FLX_CMP_SCE_FLT.SCE_CUR_COD
               ,W_FLX_CMP_SCE_FLT.CNV_CUR_RAT
               ,W_FLX_CMP_SCE_FLT.RTO_DEN_AMT
               ,W_FLX_CMP_SCE_FLT.BAS_DEN_AMT
               ,W_FLX_CMP_SCE_FLT.AMOUNT
               ,W_FLX_CMP_SCE_FLT.ACCOUNT_ELEMENT_CODE
               ,W_FLX_CMP_SCE_FLT.DESTINATION_ELEMENT_CODE
               ,W_FLX_CMP_SCE_FLT.FUNCTIONAL_AREA_ELEMENT_CODE
               ,W_FLX_CMP_SCE_FLT.CATEGORY_ELEMENT_CODE
               ,W_FLX_CMP_SCE_FLT.CHANNEL_ELEMENT_CODE
               ,W_FLX_CMP_SCE_FLT.IOM_CODE
               ,W_FLX_CMP_SCE_FLT.PLANT_CODE
               ,W_FLX_CMP_SCE_FLT.ORIGINAL_ACCOUNT_ELEMENT_CODE
               ,W_FLX_CMP_SCE_FLT.T_REC_SRC_TST
               ,W_FLX_CMP_SCE_FLT.T_REC_INS_TST
               ,TO_TIMESTAMP(CURRENT_TIMESTAMP)    T_REC_UPD_TST
    FROM        COP_DMT_FLX.W_FLX_CMP_SCE_FLT
                INNER JOIN COP_DMT_FLX.R_FLX_SCE ON
                (
                     R_FLX_SCE.CBU_COD         = W_FLX_CMP_SCE_FLT.CBU_COD         AND
                     R_FLX_SCE.SCE_ELM_COD     = W_FLX_CMP_SCE_FLT.SCE_ELM_COD     AND
                     R_FLX_SCE.ACT_SRC_SCE_COD = W_FLX_CMP_SCE_FLT.SRC_SCE_ELM_COD AND
                     R_FLX_SCE.UPD_ACT_FLG     = 1
                )
    UNION ALL
    SELECT      W_FLX_SRC_SCE__FCA_VAR.CBU_COD
               ,W_FLX_SRC_SCE__FCA_VAR.SCE_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.CFG_ORD_NUM
               ,W_FLX_SRC_SCE__FCA_VAR.SRC_SCE_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.IND_GRP_COD
               ,W_FLX_SRC_SCE__FCA_VAR.IND_GRP_RAT_FLG
               ,W_FLX_SRC_SCE__FCA_VAR.BGN_PER_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.END_PER_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.ETI_GRP_COD
               ,W_FLX_SRC_SCE__FCA_VAR.CUS_GRP_COD
               ,W_FLX_SRC_SCE__FCA_VAR.CUS_DIM_GRP_COD
               ,W_FLX_SRC_SCE__FCA_VAR.PDT_GRP_COD
               ,W_FLX_SRC_SCE__FCA_VAR.CAT_TYP_GRP_COD
               ,W_FLX_SRC_SCE__FCA_VAR.EIB_GRP_COD
               ,W_FLX_SRC_SCE__FCA_VAR.TTY_GRP_COD
               ,W_FLX_SRC_SCE__FCA_VAR.SAL_SUP_GRP_COD
               ,W_FLX_SRC_SCE__FCA_VAR.IND_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.CFG_MOD_FLG
               ,W_FLX_SRC_SCE__FCA_VAR.IND_NUM_FLG
               ,W_FLX_SRC_SCE__FCA_VAR.IND_DEN_FLG
               ,W_FLX_SRC_SCE__FCA_VAR.PER_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.ACT_PER_FLG
               ,W_FLX_SRC_SCE__FCA_VAR.ETI_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.CUS_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.PDT_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.CAT_TYP_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.EIB_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.TTY_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.SAL_SUP_ELM_COD
               ,W_FLX_SRC_SCE__FCA_VAR.SRC_CUR_COD
               ,W_FLX_SRC_SCE__FCA_VAR.SCE_CUR_COD
               ,W_FLX_SRC_SCE__FCA_VAR.CNV_CUR_RAT
               ,W_FLX_SRC_SCE__FCA_VAR.RTO_DEN_AMT
               ,W_FLX_SRC_SCE__FCA_VAR.BAS_DEN_AMT
               ,W_FLX_SRC_SCE__FCA_VAR.AMOUNT
               ,W_FLX_SRC_SCE__FCA_VAR.ACCOUNT_ELEMENT_CODE
               ,W_FLX_SRC_SCE__FCA_VAR.DESTINATION_ELEMENT_CODE
               ,W_FLX_SRC_SCE__FCA_VAR.FUNCTIONAL_AREA_ELEMENT_CODE
               ,W_FLX_SRC_SCE__FCA_VAR.CATEGORY_ELEMENT_CODE
               ,W_FLX_SRC_SCE__FCA_VAR.CHANNEL_ELEMENT_CODE
               ,W_FLX_SRC_SCE__FCA_VAR.IOM_CODE
               ,W_FLX_SRC_SCE__FCA_VAR.PLANT_CODE
               ,W_FLX_SRC_SCE__FCA_VAR.ORIGINAL_ACCOUNT_ELEMENT_CODE
               ,W_FLX_SRC_SCE__FCA_VAR.T_REC_SRC_TST
               ,W_FLX_SRC_SCE__FCA_VAR.T_REC_INS_TST
               ,TO_TIMESTAMP(CURRENT_TIMESTAMP)    T_REC_UPD_TST
    FROM        COP_DMT_FLX.W_FLX_SRC_SCE__FCA_VAR
                INNER JOIN Src_Filter ON
                (
                    W_FLX_SRC_SCE__FCA_VAR.CBU_COD     = Src_Filter.CBU_COD     AND
                    W_FLX_SRC_SCE__FCA_VAR.SCE_ELM_COD = Src_Filter.SCE_ELM_COD AND
                    W_FLX_SRC_SCE__FCA_VAR.ETI_ELM_COD = Src_Filter.ETI_ELM_COD AND
                    W_FLX_SRC_SCE__FCA_VAR.PER_ELM_COD > Src_Filter.PER_ELM_COD
                )
    ;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- Call the procedure to log the end of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

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
