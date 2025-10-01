USE DATABASE {{env}}_COP;
USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Ini_Scenario()
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
Created On  : 02-12-2024                                                 
=========================================================================
Modified On:    Description:                        Author:
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

v_STS_PROC       VARCHAR(5000) := NULL;
v_ERR_STEP       NUMBER(2,0) := 0;

BEGIN

    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Scenario_Initialization','#','DLT',CURRENT_USER);


    -- 1. UPDATE R_FLX_SCE FROM requested TO in_progress:2

    v_STEP_TABLE := 'UPDATE R_FLX_SCE FROM requested TO in_progress:2';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    UPDATE COP_DMT_FLX.R_FLX_SCE
    SET    INI_STS_COD = 'in_progress:2'
    WHERE  INI_STS_COD = 'in_progress:1';

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, :v_ERR_STEP, :v_STS_PROC, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 2. Call the procedure SP_Flex_Ini_Filter_Comp_Act

    v_STEP_TABLE := 'CALL SP_Flex_Ini_Filter_Comp_Act';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    CALL SP_Flex_Ini_Filter_Comp_Act() INTO :v_STS_PROC;

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

    -- 3. Call the procedure SP_Flex_Ini_Filter_Sources

    v_STEP_TABLE := 'SCALL P_Flex_Ini_Filter_Sources';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    CALL SP_Flex_Ini_Filter_Sources() INTO :v_STS_PROC;

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

    -- 4. Call the procedure SP_Flex_Ini_Calc_from_Ratios

    v_STEP_TABLE := 'CALL SP_Flex_Ini_Calc_from_Ratios';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    CALL SP_Flex_Ini_Calc_from_Ratios() INTO :v_STS_PROC;

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

    -- 5. Call the procedure SP_Flex_Ini_Copy_Sources_Manual

    v_STEP_TABLE := 'CALL SP_Flex_Ini_Copy_Sources_Manual';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    CALL SP_Flex_Ini_Copy_Sources_Manual() INTO :v_STS_PROC;

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

   -- 6. Call the procedure SP_Flex_Ini_Add_FCA_Var

   v_STEP_TABLE := 'CALL SP_Flex_Ini_Add_FCA_Var';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   CALL SP_Flex_Ini_Add_FCA_Var() INTO :v_STS_PROC;

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

   -- 7. Call the procedure SP_Flex_Ini_Update_Actuals

   v_STEP_TABLE := 'CALL SP_Flex_Ini_Update_Actuals';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   CALL SP_Flex_Ini_Update_Actuals() INTO :v_STS_PROC;

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

   -- 8. Call the procedure SP_Flex_Ini_Close_Gap

   v_STEP_TABLE := 'CALL SP_Flex_Ini_Close_Gap';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   CALL SP_Flex_Ini_Close_Gap() INTO :v_STS_PROC;

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

   -- 9. Call the procedure SP_Flex_Ini_Agg

   v_STEP_TABLE := 'CALL SP_Flex_Ini_Agg';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   CALL SP_Flex_Ini_Agg() INTO :v_STS_PROC;

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

   -- 10. Call the procedure SP_Flex_Ini_Pivot

   v_STEP_TABLE := 'CALL SP_Flex_Ini_Pivot';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   CALL SP_Flex_Ini_Pivot() INTO :v_STS_PROC;

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

    -- 11. UPDATE R_FLX_SCE FROM requested TO in_progress:2

    v_STEP_TABLE := 'UPDATE R_FLX_SCE FROM requested TO in_progress:3';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    UPDATE COP_DMT_FLX.R_FLX_SCE
    SET    INI_STS_COD = 'in_progress:3'
    WHERE  INI_STS_COD = 'in_progress:2';

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, :v_ERR_STEP, :v_STS_PROC, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- Call the procedure to log the end of the process
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

   -- Return text when the stored procedure finish in success
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
        WHERE  INI_STS_COD NOT IN ('created','done','failed','requested');

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
