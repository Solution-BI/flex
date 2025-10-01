USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Ind_Config_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in P_FLX_IND_CFG
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 25-02-2025                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

BEGIN

    -- Load the table P_FLX_IND_CFG in frp mode

    TRUNCATE TABLE COP_DMT_FLX.P_FLX_IND_CFG;

    INSERT INTO COP_DMT_FLX.P_FLX_IND_CFG
          (IND_ELM_COD
          ,CFG_MOD_FLG
          ,ACC01_L5_GL_ACCOUNT_CODE
          ,ACC01_L3_SUB_CATEGORY_CODE
          ,ACC01_L1_MACRO_CATEGORY_CODE
          ,ACC01_ACCOUNT_TYPE_CODE
          ,ACC02_ACC_TOP_NOD
          ,ACC03_ACC_TOP_NOD
          ,L1_CUSTOMER_DISTRIBUTION_CHANNEL_DESC
          ,DST01_L3_DESTINATION_CODE
          ,DST01_L2_DESTINATION_CODE
          ,DST01_L1_DESTINATION_CODE
          ,FCT_ARE_ELM_COD
          ,FCT_ARE_LVL1_NOD
          ,IOM_TYP_COD
          ,T_REC_ARC_FLG
          ,T_REC_DLT_FLG
          ,T_REC_SRC_TST
          ,T_REC_INS_TST
          ,T_REC_UPD_TST
          )
    SELECT   IND_ELM_COD
            ,CFG_MOD_FLG
            ,ACC01_L5_GL_ACCOUNT_CODE
            ,ACC01_L3_SUB_CATEGORY_CODE
            ,ACC01_L1_MACRO_CATEGORY_CODE
            ,ACC01_ACCOUNT_TYPE_CODE
            ,ACC02_ACC_TOP_NOD
            ,ACC03_ACC_TOP_NOD
            ,L1_CUSTOMER_DISTRIBUTION_CHANNEL_DESC
            ,DST01_L3_DESTINATION_CODE
            ,DST01_L2_DESTINATION_CODE
            ,DST01_L1_DESTINATION_CODE
            ,FCT_ARE_ELM_COD
            ,FCT_ARE_LVL1_NOD
            ,IOM_TYP_COD
            ,T_REC_ARC_FLG
            ,T_REC_DLT_FLG
            ,T_REC_SRC_TST
            ,T_REC_INS_TST
            ,T_REC_UPD_TST
    FROM     COP_DMT_CCD.P_CCD_IND_CFG_UNIFY;

    -- Return text when the stored procedure completes

    RETURN 'Success';
EXCEPTION
    WHEN OTHER THEN
         RETURN SQLCODE || ': ' || SQLERRM;
         RAISE; -- Raise the same exception that you are handling.
END;
$$
;
