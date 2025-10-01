USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Functional_Area_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_FCT_ARE
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 25-02-2025                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

BEGIN

    -- Load the table R_FLX_FCT_ARE in frp mode

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_FCT_ARE;

    INSERT INTO COP_DMT_FLX.R_FLX_FCT_ARE
          (CBU_COD
          ,FCT_ARE_ELM_COD
          ,FCT_ARE_ELM_DSC
          ,LVL1_NOD
          ,T_REC_DLT_FLG
          ,T_REC_INS_TST
          ,T_REC_UPD_TST
          )
    SELECT DISTINCT 
           CBU_COD
          ,FCT_ARE_ELM_COD
          ,FCT_ARE_ELM_DSC
          ,LVL1_NOD
          ,0                                                                              T_REC_DLT_FLG
          ,CURRENT_TIMESTAMP                                                              T_REC_INS_TST
          ,CURRENT_TIMESTAMP                                                              T_REC_UPD_TST
    FROM   COP_DSP_CONTROLLING_CLOUD.R_FCT_ARE_UNIFY;

    -- Return text when the stored procedure completes

    RETURN 'Success';
EXCEPTION
    WHEN OTHER THEN
         RETURN SQLCODE || ': ' || SQLERRM;
         RAISE; -- Raise the same exception that you are handling.
END;
$$
;
