USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE PROCEDURE SP_Flex_Load_EIB_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_EIB
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 27-06-2024                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);

BEGIN

   -- Load the table R_FLX_EIB in frp mode

   v_STEP_TABLE := 'R_FLX_EIB';

   TRUNCATE TABLE COP_DMT_FLX.R_FLX_EIB;

   INSERT INTO COP_DMT_FLX.R_FLX_EIB
         (EIB_ELM_KEY
         ,EIB_ELM_COD
         ,EIB_ELM_DSC
         ,CBU_COD
         ,T_REC_DLT_FLG
         ,T_REC_INS_TST
         ,T_REC_UPD_TST)
   SELECT CBU_COD || '-' || EIB_ELEMENT_CODE      EIB_ELM_KEY
         ,EIB_ELEMENT_CODE                        EIB_ELM_COD
         ,EIB_ELEMENT_DESC                        EIB_ELM_DSC
         ,CBU_COD                                 CBU_COD
         ,0                                       T_REC_DLT_FLG
         ,CURRENT_TIMESTAMP                       T_REC_INS_TST
         ,CURRENT_TIMESTAMP                       T_REC_UPD_TST
   FROM   PRD_COP.COP_DSP_CONTROLLING_CLOUD.R_EIB_UNIFY;

   v_STEP_TABLE := 'R_FLX_GRP_EIB';

   TRUNCATE TABLE COP_DMT_FLX.R_FLX_GRP_EIB;

   INSERT OVERWRITE ALL
   INTO COP_DMT_FLX.R_FLX_GRP_EIB
        (CBU_COD, EIB_ELM_COD, EIB_DIM_GRP_COD, EIB_GRP_COD, EIB_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        VALUES (CBU_COD, EIB_ELM_COD, -1, '$TOTAL_EIB', 'Total Business Type', T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
   INTO COP_DMT_FLX.R_FLX_GRP_EIB
        (CBU_COD, EIB_ELM_COD, EIB_DIM_GRP_COD, EIB_GRP_COD, EIB_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        VALUES (CBU_COD, EIB_ELM_COD,  0, EIB_ELM_COD, EIB_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
   SELECT   CBU_COD
           ,EIB_ELM_COD
           ,EIB_ELM_DSC
           ,0                                           as T_REC_DLT_FLG
           ,current_timestamp                           as T_REC_INS_TST
           ,current_timestamp                           as T_REC_UPD_TST
   FROM     COP_DMT_FLX.R_FLX_EIB
   ORDER BY CBU_COD, EIB_ELM_COD;

  -- Return text when the stored procedure completes

  RETURN 'Success';
EXCEPTION
   WHEN OTHER THEN
        RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
        RAISE; -- Raise the same exception that you are handling.
END;
$$
;
