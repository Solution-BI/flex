USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE PROCEDURE SP_Flex_Writeback_Copy_to_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in F_FLX_SCE_CCD
Author      : Noël Coquio (Solution BI France)
Created On  : 19-09-2024
=========================================================================
Modified On:      Description:    still in DEV PART           Author:
=========================================================================
*/

DECLARE
v_STEP_TABLE         VARCHAR(256);
v_STEP_TABLE_ERR     VARCHAR(256);
V_RUN_ID             VARCHAR(256);
v_STEP_NUM           NUMBER(5,0)  := 0;
V_STEP_BEG_DT        VARCHAR(50);
V_STEP_END_DT        VARCHAR(50);
v_ERR_MSG            VARCHAR(1000);
v_IS_SCENARIO        INTEGER;

v_STS_PROC           VARCHAR(5000);
v_ERR_STEP           NUMBER(2,0);
BEGIN

    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Writeback_Copy_to_CC','#','DLT',CURRENT_USER);

    v_STEP_TABLE := 'Update all scenarios in_progress:2 to in_progress:3';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    -- 1. Update all scenarios in_progress:2 to in_progress:3

    UPDATE COP_DMT_FLX.R_FLX_SCE
    SET    CCD_STS_COD = 'in_progress:3'
    WHERE  CCD_STS_COD = 'in_progress:2';

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 2. Delete the table F_CCD_FLX_DTL_UNIFY

    v_STEP_TABLE := 'DELETE SCENARIO FROM COP_DMT_CCD.F_CCD_FLX_DTL_UNIFY';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    DELETE FROM COP_DMT_CCD.F_CCD_FLX_DTL_UNIFY
    WHERE  (CBU_COD
           ,SCE_ELM_COD) IN (SELECT DISTINCT
                                    CBU_COD
                                   ,SCE_ELM_COD
                             FROM   COP_DMT_FLX.W_FLX_SCE_CCD);

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 3. Load the data in the table F_CCD_FLX_DTL_PNL

    v_STEP_TABLE := 'INSERT INTO COP_DMT_CCD.F_CCD_FLX_DTL_PNL';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    INSERT INTO COP_DMT_CCD.F_CCD_FLX_DTL_UNIFY
          (CBU_COD
          ,SCE_ELM_COD
          ,PER_ELM_COD
          ,ETI_ELM_COD
          ,ACC_ELM_COD
          ,DST_ELM_COD
          ,CUS_ELM_COD
          ,IOM_COD
          ,CHL_ELM_COD
          ,PDT_ELM_COD
          ,BUS_TYP_COD
          ,CAT_ELM_COD
          ,EXT_TYP_COD
          ,TTY_COD
          ,PLT_COD
          ,RVN_AMT_ACT_VAL
          ,CUR_COD
          ,T_REC_ARC_FLG
          ,T_REC_DLT_FLG
          ,T_REC_SRC_TST
          ,T_REC_INS_TST
          ,T_REC_UPD_TST
          ,FCT_ARE
          )
    SELECT CBU_COD
          ,SCE_ELM_COD
          ,PER_ELM_COD
          ,ETI_ELM_COD
          ,ACC_ELM_COD
          ,DST_ELM_COD
          ,CUS_ELM_COD
          ,IOM_COD
          ,CHL_ELM_COD
          ,PDT_ELM_COD
          ,BUS_TYP_COD
          ,CAT_ELM_COD
          ,EXT_TYP_COD
          ,TTY_COD
          ,PLT_COD
          ,RVN_AMT_ACT_VAL
          ,CUR_COD
          ,T_REC_ARC_FLG
          ,T_REC_DLT_FLG
          ,T_REC_SRC_TST
          ,T_REC_INS_TST
          ,T_REC_UPD_TST
          ,FCT_ARE_ELM_COD
    FROM   COP_DMT_FLX.W_FLX_SCE_CCD;

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
         v_STEP_TABLE_ERR := v_STEP_TABLE;

         -- Assign the end date of the step
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

         -- Call the procedure to log the step in error with the error message
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, -1, :v_ERR_MSG, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         v_STEP_TABLE := 'UPDATE STATUS TO failed IN R_FLX_SCE';
         v_STEP_NUM   := v_STEP_NUM + 1;

         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

         -- xx. Update all scenarios init_in_progress to failed in case of failure
         UPDATE COP_DMT_FLX.R_FLX_SCE
         SET    CCD_STS_COD = 'failed'
         WHERE  CCD_STS_COD IN ('in_progress:2','in_progress:3');

         -- Assign the end date of the step
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

         -- Call the procedure to log the step
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         -- Call the procedure to log the end of the process
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

         v_ERR_MSG := v_ERR_MSG || ' in the step ' || v_STEP_TABLE_ERR;

         RETURN v_ERR_MSG;
         RAISE; -- Raise the same exception that you are handling.

END;
$$
;
