USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Destination_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_DESTINATION
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 25-02-2025                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

BEGIN

    -- Load the table R_FLX_DESTINATION in frp mode

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_DESTINATION;

    INSERT INTO COP_DMT_FLX.R_FLX_DESTINATION
          (CBU_COD
          ,DST_ELM_KEY
          ,DST_ELM_COD
          ,DST_ELM_DSC
          ,DST01_L3_DESTINATION_CODE
          ,DST01_L2_DESTINATION_CODE
          ,DST01_L1_DESTINATION_CODE 
          ,T_REC_DLT_FLG
          ,T_REC_INS_TST
          ,T_REC_UPD_TST
          )
    SELECT DISTINCT 
           CBU_COD
          ,DST_KEY_COD                  AS DST_ELM_KEY
          ,DST_ELM_COD
          ,DST_ELM_DSC
          ,DST01_L3_DESTINATION_CODE
          ,DST01_L2_DESTINATION_CODE
          ,DST01_L1_DESTINATION_CODE 
          ,0                            AS T_REC_DLT_FLG
          ,CURRENT_TIMESTAMP            AS T_REC_INS_TST
          ,CURRENT_TIMESTAMP            AS T_REC_UPD_TST
    FROM   COP_DSP_CONTROLLING_CLOUD.R_DESTINATION_UNIFY;

    -- Return text when the stored procedure completes

    RETURN 'Success';
EXCEPTION
    WHEN OTHER THEN
         RETURN SQLCODE || ': ' || SQLERRM;
         RAISE; -- Raise the same exception that you are handling.
END;
$$
;
