USE SCHEMA COP_DMT_FLX{{uid}};

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Deleting Manual Dataset

Author      : Noël COQUIO (Solution BI France)
Created On  : 05-03-2025
=========================================================================
Modified On:    Description:                        Author:
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Delete_Manual_Dataset()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$

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
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Delete_Manual_Dataset','#','DLT',CURRENT_USER);

   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'Check Manual Dataset to delete';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   -- 1. Check if there is scenario in progress of deletion

   SELECT Count(*)
   INTO   v_IS_SCENARIO
   FROM   COP_DMT_FLX.R_FLX_Man_SCE
   WHERE  MAN_SCE_DLT_STS_COD = 'requested';

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   IF (v_IS_SCENARIO = 0) THEN
      -- Call the procedure to log the end of the process
      CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

      RETURN 'No Manual Dataset to delete currently...';
   END IF;

   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'Update Deletion Status';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   -- Update deletion status

   UPDATE COP_DMT_FLX.R_FLX_Man_SCE
   SET    MAN_SCE_DLT_STS_COD = 'in_progress:1'
   WHERE  MAN_SCE_DLT_STS_COD = 'requested';

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);


   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'Delete Manual Dataset in F_FLX_MAN_SCE';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   -- Delete Scenario in Comparable Table

   DELETE FROM F_FLX_MAN_SCE
   WHERE  MAN_SCE_ELM_KEY IN (SELECT MAN_SCE_ELM_KEY
                              FROM   R_FLX_MAN_SCE
                              WHERE  MAN_SCE_DLT_STS_COD = 'in_progress:1');
    
   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'Delete Manual Dataset in R_FLX_MAN_SCE';
   v_STEP_NUM   := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   -- Delete Scenario in F_FLX_SRC_SCE

   DELETE FROM R_FLX_MAN_SCE
   WHERE  MAN_SCE_DLT_STS_COD = 'in_progress:1';
    
   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- Call the procedure to log the step
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

        -- Call the procedure to log the step
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

        UPDATE R_FLX_SCE
        SET    MAN_SCE_DLT_STS_COD = 'failed'
              ,MAN_SCE_DLT_END_TST = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP())
        WHERE  MAN_SCE_DLT_STS_COD = 'in_progress:1';
        
        RETURN v_ERR_MSG;
        RAISE; -- Raise the same exception that you are handling.

END;
$$
;