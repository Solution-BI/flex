USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Category_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_CAT
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 27-06-2024                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);
BEGIN

   -- Load the table R_FLX_ETI in frp mode

   v_STEP_TABLE := 'R_FLX_CAT';

   TRUNCATE TABLE COP_DMT_FLX.R_FLX_CAT;

   INSERT INTO COP_DMT_FLX.R_FLX_CAT
         (CAT_ELM_KEY
         ,CAT_ELM_COD
         ,CAT_ELM_DSC
         ,CBU_COD
         ,CAT_TYP_COD
         ,T_REC_DLT_FLG
         ,T_REC_INS_TST
         ,T_REC_UPD_TST
         )
   VALUES ('DCH-NA','NA','NA','DCH','NA',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
         ,('IBE-NA','NA','NA','IBE','NA',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
         ,('POL-NA','NA','NA','POL','NA',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
   ;


   v_STEP_TABLE := 'R_FLX_GRP_CAT';

   TRUNCATE TABLE COP_DMT_FLX.R_FLX_GRP_CAT;

   INSERT OVERWRITE ALL
   INTO COP_DMT_FLX.R_FLX_GRP_CAT
        (CBU_COD, CAT_ELM_COD, CAT_DIM_GRP_COD, CAT_GRP_COD, CAT_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        VALUES (CBU_COD, CAT_ELM_COD, -1, '$TOTAL_CAT', 'Total Category', T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
   INTO COP_DMT_FLX.R_FLX_GRP_CAT
        (CBU_COD, CAT_ELM_COD, CAT_DIM_GRP_COD, CAT_GRP_COD, CAT_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        VALUES (CBU_COD, CAT_ELM_COD,  0, CAT_ELM_COD, CAT_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
   SELECT   CBU_COD
           ,CAT_ELM_COD
           ,CAT_ELM_DSC
           ,0                                           as T_REC_DLT_FLG
           ,current_timestamp                           as T_REC_INS_TST
           ,current_timestamp                           as T_REC_UPD_TST
   FROM     COP_DMT_FLX.R_FLX_CAT
   ORDER BY CBU_COD, CAT_ELM_COD;

  -- Return text when the stored procedure completes

  RETURN 'Success';

EXCEPTION
   WHEN OTHER THEN
        RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
        RAISE; -- Raise the same exception that you are handling.
END
$$;
;
