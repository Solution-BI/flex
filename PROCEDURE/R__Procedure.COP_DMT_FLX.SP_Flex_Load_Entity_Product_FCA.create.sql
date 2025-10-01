USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Entity_Product_FCA()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_ETI_PDT_FCA
              Fixed Cogs Adjustment values at the CBU entity & product level
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 18-12-2024                                                 
=========================================================================
Modified On:      Description:                                 Author:          
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
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Load_Entity_Product_FCA','#','FULL',CURRENT_USER);

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'TRUNCATE R_FLX_ETI_PDT_FCA';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_ETI_PDT_FCA;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- Assign the step name, step num and start date of the step

    v_STEP_TABLE := 'INSERT INTO R_FLX_ETI_PDT_FCA from P_FLX_ETI_PDT_FCA';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    -- Load the table R_FLX_ETI_PDT_FCA in full mode
    INSERT INTO COP_DMT_FLX.R_FLX_ETI_PDT_FCA
               (CBU_COD
               ,ETI_ELM_COD
               ,PDT_ELM_COD
               ,CFG_ORD_NUM
               ,FCA_MAT_OTH_VAL
               ,FCA_MANUF_OTH_VAL
               ,FCA_LOG_OTH_VAL
               ,T_REC_DLT_FLG
               ,T_REC_INS_TST
               ,T_REC_UPD_TST
               )
    SELECT      DISTINCT
                P_FLX_ETI_PDT_FCA.CBU_COD
               ,P_FLX_ETI_PDT_FCA.ETI_ELM_COD
               ,R_FLX_GRP_PDT.PDT_ELM_COD
               ,P_FLX_ETI_PDT_FCA.CFG_ORD_NUM
               ,P_FLX_ETI_PDT_FCA.FCA_MAT_OTH_VAL
               ,P_FLX_ETI_PDT_FCA.FCA_MANUF_OTH_VAL
               ,P_FLX_ETI_PDT_FCA.FCA_LOG_OTH_VAL
                ,0                                   AS T_REC_DLT_FLG
               ,CURRENT_TIMESTAMP                    AS T_REC_INS_TST
               ,CURRENT_TIMESTAMP                    AS T_REC_UPD_TST
    FROM        COP_DMT_FLX.P_FLX_ETI_PDT_FCA
                INNER JOIN COP_DMT_FLX.R_FLX_GRP_PDT ON
                (
                  R_FLX_GRP_PDT.CBU_COD     = P_FLX_ETI_PDT_FCA.CBU_COD     AND
                  R_FLX_GRP_PDT.PDT_GRP_COD = P_FLX_ETI_PDT_FCA.PDT_GRP_COD
                )
    WHERE       P_FLX_ETI_PDT_FCA.T_REC_DLT_FLG      = 0
    AND        (P_FLX_ETI_PDT_FCA.FCA_LOG_OTH_VAL   != 0
    OR          P_FLX_ETI_PDT_FCA.FCA_MANUF_OTH_VAL != 0
    OR          P_FLX_ETI_PDT_FCA.FCA_MAT_OTH_VAL   != 0)
    QUALIFY     P_FLX_ETI_PDT_FCA.CFG_ORD_NUM        = MAX(P_FLX_ETI_PDT_FCA.CFG_ORD_NUM) 
                                                       OVER (PARTITION BY P_FLX_ETI_PDT_FCA.CBU_COD
                                                                         ,P_FLX_ETI_PDT_FCA.ETI_ELM_COD
                                                                         ,R_FLX_GRP_PDT.PDT_ELM_COD
                                                            )
   ;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- Call the procedure to log the end of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

   -- Return text when the stored procedure completes
   RETURN 'Success';

EXCEPTION
    WHEN OTHER THEN

         v_ERR_MSG := REPLACE(SQLCODE || ': ' || SQLERRM,'''','"');

         -- Assign the end date of the step
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

         -- Call the procedure to log the step in error with the error message
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, -1, :v_ERR_MSG, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         -- Call the procedure to log the end of the process
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);
        
         RETURN v_ERR_MSG;
         RAISE; -- Raise the same exception that you are handling.

END;
$$
;
