USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Writeback_to_CC_setup()
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

v_STS_PROC       VARCHAR(5000);
v_ERR_STEP       NUMBER(2,0);
BEGIN

    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Writeback_to_CC_setup','#','DLT',CURRENT_USER);

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'CHECK DATA TO TRANSFER';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    -- 1. Check if there is scenario requested

    SELECT Count(*)
    INTO   v_IS_SCENARIO
    FROM   COP_DMT_FLX.R_FLX_SCE
    WHERE  CCD_STS_COD = 'requested';

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    IF (v_IS_SCENARIO = 0) THEN
        -- Call the procedure to log the end of the process
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

        RETURN 'Aucun Scénario configuré à copier actuellement...';
    END IF;

    v_STEP_TABLE := 'UPDATE STATUS TO done IN R_FLX_SCE';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    -- 5. Update all scenarios in requested to in_progress:1

    UPDATE COP_DMT_FLX.R_FLX_SCE
    SET    CCD_STS_COD = 'in_progress:1'
          ,CCD_END_TST = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    WHERE  CCD_STS_COD = 'requested';

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- W_FLX_SCE_EML__CCD  Working table to send the email at the end of the initialization process
    v_STEP_NUM   := v_STEP_NUM + 1;
    v_STEP_TABLE := 'Truncate working table W_FLX_SCE_EML__CCD ';

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SCE_EML__CCD ;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 2. Load the scenario information to send the email

    v_STEP_NUM   := v_STEP_NUM + 1;
    v_STEP_TABLE := 'load email data in the working table W_FLX_SCE_EML__CCD ';

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    INSERT INTO W_FLX_SCE_EML__CCD
               (SCE_ELM_KEY
               ,SCE_ELM_DSC
               ,EML_USR_COD
               ,EML_FLG
               ,T_REC_UPD_TST
               )
    SELECT   SCE_ELM_KEY
            ,REPLACE(REPLACE(SCE_ELM_DSC,'''','`'),'"','``')    SCE_ELM_DSC
            ,CCD_RQT_EML_USR_COD                                EML_USR_COD
            ,0                                                  EML_FLG
            ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                T_REC_UPD_TST
    FROM     R_FLX_SCE
    WHERE    CCD_STS_COD NOT IN ('na','done','failed','requested');

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- Call the procedure to log the end of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

    RETURN 'Success';

EXCEPTION
   WHEN OTHER THEN

        v_ERR_MSG := SQLCODE || ': ' || SQLERRM;

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

        RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
        RAISE; -- Raise the same exception that you are handling.

END;
$$
;
