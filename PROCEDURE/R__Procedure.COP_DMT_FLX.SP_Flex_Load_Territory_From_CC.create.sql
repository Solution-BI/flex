USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Territory_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_TTY
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 10-07-2024                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);

BEGIN

   -- Load the table R_FLX_TTY in frp mode

   v_STEP_TABLE := 'R_FLX_TTY';

   TRUNCATE TABLE COP_DMT_FLX.R_FLX_TTY;
   
   INSERT INTO COP_DMT_FLX.R_FLX_TTY
         (TTY_ELM_KEY
         ,TTY_ELM_COD
         ,TTY_ELM_DSC
         ,CBU_COD
         ,T_REC_DLT_FLG
         ,T_REC_INS_TST
         ,T_REC_UPD_TST
         )
   VALUES ('DCH-NA','NA','NA','DCH',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
         ,('IBE-NA','NA','NA','IBE',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
         ,('POL-NA','NA','NA','POL',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
   

   v_STEP_TABLE := 'R_FLX_GRP_TTY';

   TRUNCATE TABLE COP_DMT_FLX.R_FLX_GRP_TTY;

   INSERT OVERWRITE ALL
   INTO COP_DMT_FLX.R_FLX_GRP_TTY
        (CBU_COD, TTY_ELM_COD, TTY_DIM_GRP_COD, TTY_GRP_COD, TTY_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        VALUES (CBU_COD, TTY_ELM_COD, -1, '$TOTAL_TTY', 'Total Territory', T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
   INTO COP_DMT_FLX.R_FLX_GRP_TTY
        (CBU_COD, TTY_ELM_COD, TTY_DIM_GRP_COD, TTY_GRP_COD, TTY_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        VALUES (CBU_COD, TTY_ELM_COD,  0, TTY_ELM_COD, TTY_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
   SELECT   CBU_COD
           ,TTY_ELM_COD
           ,TTY_ELM_DSC
           ,0                                           as T_REC_DLT_FLG
           ,current_timestamp                           as T_REC_INS_TST
           ,current_timestamp                           as T_REC_UPD_TST
   FROM     COP_DMT_FLX.R_FLX_TTY
   ORDER BY CBU_COD, TTY_ELM_COD;

  -- Return text when the stored procedure completes

  RETURN 'Success';

EXCEPTION
   WHEN OTHER THEN
        RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
        RAISE; -- Raise the same exception that you are handling.
END;
$$
;
