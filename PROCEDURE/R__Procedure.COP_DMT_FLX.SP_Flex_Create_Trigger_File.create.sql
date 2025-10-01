USE DATABASE {{env}}_COP;
USE SCHEMA COP_DMT_FLX;

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For create a trigger file depending on the interface code

Author      : Noel Coquio (Solution BI France)
Created On  : 03-10-2024
=========================================================================
Modified On:    Description:                        Author:
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Create_Trigger_File(ITF_COD VARCHAR(256))
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS CALLER
AS
$$
DECLARE

v_ERR_MSG        VARCHAR(1000) := '';
v_error          NUMBER(2,0) := 0;
V_RUN_ID         VARCHAR(256);
V_STEP_BEG_DT    VARCHAR(50);
V_STEP_END_DT    VARCHAR(50);

BEGIN

   -- Generate the UUID for the procedure
   CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   IF (ITF_COD = 'CCD2FLX') THEN
      BEGIN
         REMOVE @STG_FLX_TRF_FIL/TRG_CC_TO_FLX.csv;
      EXCEPTION
         WHEN OTHER THEN
              v_error := -1;
              v_ERR_MSG := v_ERR_MSG || 'Unable to delete the file TRG_CC_TO_FLX.csv: ' || SQLCODE || '. ';
      END;
 
      BEGIN
         COPY INTO @STG_FLX_TRF_FIL/TRG_CC_TO_FLX.csv FROM (SELECT 1) SINGLE = TRUE;
      EXCEPTION
         WHEN OTHER THEN
              v_error := -1;
              v_ERR_MSG := v_ERR_MSG || 'Unable to create the file TRG_CC_TO_FLX.csv: ' || SQLCODE || '. ';
      END;

   ELSEIF (ITF_COD = 'FLX2CCD') THEN
      BEGIN
         REMOVE @STG_FLX_TRF_FIL/TRG_FLX_TO_CC.csv;
      EXCEPTION
         WHEN OTHER THEN
              v_error := -1;
              v_ERR_MSG := v_ERR_MSG || 'Unable to delete the file TRG_FLX_TO_CC.csv: ' || SQLCODE || '. ';
      END;
 
      BEGIN
         COPY INTO @STG_FLX_TRF_FIL/TRG_FLX_TO_CC.csv FROM (SELECT 1) SINGLE = TRUE;
      EXCEPTION
         WHEN OTHER THEN
              v_error := -1;
              v_ERR_MSG := v_ERR_MSG || 'Unable to create the file TRG_FLX_TO_CC.csv: ' || SQLCODE || '. ';
      END;

   ELSE
      BEGIN
         REMOVE @STG_FLX_TRF_FIL/empty_file.csv;
      EXCEPTION
         WHEN OTHER THEN
              v_error := -1;
              v_ERR_MSG := v_ERR_MSG || 'Unable to delete the file empty_file.csv: ' || SQLCODE || '. ';
      END;
 
      BEGIN
         COPY INTO @STG_FLX_TRF_FIL/empty_file.csv FROM (SELECT 1) SINGLE = TRUE;
      EXCEPTION
         WHEN OTHER THEN
              v_error := -1;
              v_ERR_MSG := v_ERR_MSG || 'Unable to create the file empty_file.csv: ' || SQLCODE || '. ';
      END;

   END IF;
   
   if ( v_error = 0 ) THEN
      v_ERR_MSG := 'Success';
   END IF; 

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, 'SP_Flex_Create_Trigger_File', 1, :v_error, :v_ERR_MSG, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   RETURN v_ERR_MSG;

EXCEPTION
   WHEN OTHER THEN
        v_ERR_MSG := REPLACE(SQLCODE || ': ' || SQLERRM,'''','"');
        RETURN v_ERR_MSG;

END;

$$
;