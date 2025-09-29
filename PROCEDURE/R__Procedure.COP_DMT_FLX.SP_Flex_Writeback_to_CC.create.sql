USE SCHEMA COP_DMT_FLX{{uid}};

DROP PROCEDURE IF EXISTS SP_FLEX_LOAD_DEAGGREGATION_DATA_FROM_POWERON();

CREATE OR REPLACE PROCEDURE SP_Flex_Writeback_to_CC()
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
v_STEP_TABLE     VARCHAR(256);
V_RUN_ID         VARCHAR(256);
v_STEP_NUM       NUMBER(5,0)  := 0;
V_STEP_BEG_DT    VARCHAR(50);
V_STEP_END_DT    VARCHAR(50);
v_ERR_MSG        VARCHAR(1000);
v_IS_SCENARIO    INTEGER;

v_STS_PROC       VARCHAR(5000) := NULL;
v_ERR_STEP       NUMBER(2,0) := 0;
BEGIN

    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Writeback_to_CC','#','DLT',CURRENT_USER);

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'CHECK DATA TO TRANSFER';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    -- 1. Check if there is scenario in progress of deletion

    SELECT Count(*)
    INTO   v_IS_SCENARIO
    FROM   COP_DMT_FLX.R_FLX_SCE
    WHERE  CCD_STS_COD NOT IN ('na','done','failed','requested');

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, :v_ERR_STEP, :v_STS_PROC, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    IF (v_IS_SCENARIO = 0) THEN
        -- Call the procedure to log the end of the process
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

        RETURN 'Aucun Scénario à transférer actuellement...';
    END IF;

    -- CALL the procedure SP_Flex_Writeback_Disaggregate'
   
    v_STEP_TABLE := 'CALL the procedure SP_Flex_Writeback_Disaggregate';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    CALL COP_DMT_FLX.SP_Flex_Writeback_Disaggregate() INTO :v_STS_PROC;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    IF (:v_STS_PROC != 'Success') THEN
        v_ERR_STEP := -1;
        -- Call the procedure to log the step
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, :v_ERR_STEP, :v_STS_PROC, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);
        -- Call the procedure to log the end of the process
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

        -- 4. Return text when the stored procedure finish in success
        RETURN :v_STS_PROC;
    ELSE
        v_STS_PROC := NULL;
    END IF;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, :v_ERR_STEP, :v_STS_PROC, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 4. Call the procedure SP_Flex_Writeback_Copy_to_CC to copy the scenario to CC
    v_STEP_TABLE := 'CALL the procedure SP_Flex_Writeback_Copy_to_CC to copy the scenario to CC';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    CALL COP_DMT_FLX.SP_Flex_Writeback_Copy_to_CC() INTO :v_STS_PROC;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    IF (:v_STS_PROC != 'Success') THEN
        v_ERR_STEP := -1;
        -- Call the procedure to log the step
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, :v_ERR_STEP, :v_STS_PROC, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);
        -- Call the procedure to log the end of the process
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

        -- 4. Return text when the stored procedure finish in success
        RETURN :v_STS_PROC;
    ELSE
        v_STS_PROC := NULL;
    END IF;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, :v_ERR_STEP, :v_STS_PROC, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    v_STEP_TABLE := 'UPDATE STATUS TO done IN R_FLX_SCE';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    -- 5. Update all scenarios init_in_progress to copy_done in case of success

    UPDATE COP_DMT_FLX.R_FLX_SCE
    SET    CCD_STS_COD = 'done'
          ,CCD_END_TST = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    WHERE  CCD_STS_COD = 'in_progress:3';

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, :v_ERR_STEP, :v_STS_PROC, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

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

         v_STEP_TABLE := 'UPDATE STATUS TO copy_failed IN R_FLX_SCE';
         v_STEP_NUM   := v_STEP_NUM + 1;

         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

         -- xx. Update all scenarios init_in_progress to copy_failed in case of failure
         UPDATE COP_DMT_FLX.R_FLX_SCE
         SET    CCD_STS_COD = 'failed'
         WHERE  CCD_STS_COD NOT IN ('na','done','failed','requested');

         -- Assign the end date of the step
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

         -- Call the procedure to log the step
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         -- Call the procedure to log the end of the process
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

         v_ERR_MSG := v_ERR_MSG || ' in the step ' || v_STEP_TABLE;

         RETURN v_ERR_MSG;
         RAISE; -- Raise the same exception that you are handling.

END;
$$
;
