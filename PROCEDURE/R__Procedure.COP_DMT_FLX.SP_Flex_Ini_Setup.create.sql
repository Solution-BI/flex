USE SCHEMA COP_DMT_FLX;


CREATE OR REPLACE PROCEDURE SP_Flex_Ini_Setup()
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
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Ini_Setup','#','DLT',CURRENT_USER);

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'CHECK DATA TO TRANSFER';
    v_STEP_NUM   := v_STEP_NUM + 1;

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

    -- Assign the step name, step num and start date of the step

    v_STEP_TABLE := 'UPDATE R_FLX_SCE FROM requested TO in_progress:1';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    -- 2. Update all scenarios requested to in_progress:1

    UPDATE COP_DMT_FLX.R_FLX_SCE
    SET    INI_STS_COD = 'in_progress:1'
    WHERE  INI_STS_COD = 'requested';

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 3. Truncate all the working tables used by the process

    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    v_STEP_TABLE := 'Truncate working tables W_FLX_SRC_SCE, W_FLX_SRC_SCE__FLT, W_FLX_SCE_SIM__OUT, W_FLX_SCE_SIM__IN_INI and W_FLX_SCE_EML__INI';
    -- W_FLX_SRC_SCE Working table to load the source scenario from CCD
    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SRC_SCE;

    -- W_FLX_SRC_SCE__FLT Working table to load the scenario to filter
    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SRC_SCE__FLT;

    -- W_FLX_SCE_SIM__OUT Working table to load the scenario from Flex
    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SCE_SIM__OUT;

    -- W_FLX_SCE_SIM__IN_INI  Working table to load the scenario for Flex
    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SCE_SIM__IN_INI ;

    -- W_FLX_SCE_EML__INI  Working table to send the email at the end of the initialization process
    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SCE_EML__INI ;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 4. Load the scenario information to send the email

    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    v_STEP_TABLE := 'Load working table W_FLX_SCE_EML__INI';

    INSERT INTO W_FLX_SCE_EML__INI
               (SCE_ELM_KEY
               ,SCE_ELM_DSC
               ,EML_USR_COD
               ,EML_FLG
               ,T_REC_UPD_TST
               )
    SELECT   SCE_ELM_KEY
            ,REPLACE(REPLACE(SCE_ELM_DSC,'''','`'),'"','``')    SCE_ELM_DSC
            ,INI_RQT_EML_USR_COD                                EML_USR_COD
            ,0                                                  EML_FLG
            ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)                T_REC_UPD_TST
    FROM     R_FLX_SCE
    WHERE    INI_STS_COD = 'in_progress:1';

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
