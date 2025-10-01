USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Account_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_ACCOUNT
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 25-02-2025                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

BEGIN

    -- Load the table R_FLX_ACCOUNT in frp mode

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_ACCOUNT;

    INSERT INTO COP_DMT_FLX.R_FLX_ACCOUNT
          (CBU_COD
          ,ACC_ELM_KEY
          ,ACC_ELM_COD
          ,ACC_ELM_DSC
          ,ACC01_L5_GL_ACCOUNT_CODE
          ,ACC01_L3_SUB_CATEGORY_CODE
          ,ACC01_L1_MACRO_CATEGORY_CODE
          ,ACC01_ACCOUNT_TYPE_CODE
          ,ACC02_ACC_TOP_NOD
          ,ACC03_ACC_TOP_NOD
          ,T_REC_DLT_FLG
          ,T_REC_INS_TST
          ,T_REC_UPD_TST
          )
    WITH DSP_ACC AS (
        SELECT   R_CCD_ACC_UNIFY.CBU_COD
                ,R_CCD_ACC_UNIFY.ACC_ELM_COD                                                                      AS ACC_ELM_KEY
                ,REPLACE(REPLACE(R_CCD_ACC_UNIFY.ACC_ELM_COD,'ACC_GL_',''),'ACC_','')                             AS ACC_ELM_COD
                ,MAX(R_CCD_ACC_UNIFY.ACC_ELM_DSC)                                                                 AS ACC_ELM_DSC
                ,REPLACE(REPLACE(MAX(IFF(CONTAINS(TOP_NOD,'ACC_SC_TOT'),LVL6_NOD,NULL)),'ACC_GL_',''),'ACC_','')  AS ACC01_L5_GL_ACCOUNT_CODE
                ,REPLACE(REPLACE(MAX(IFF(CONTAINS(TOP_NOD,'ACC_SC_TOT'),LVL4_NOD,NULL)),'ACC_GL_',''),'ACC_','')  AS ACC01_L3_SUB_CATEGORY_CODE
                ,REPLACE(REPLACE(MAX(IFF(CONTAINS(TOP_NOD,'ACC_SC_TOT'),LVL2_NOD,NULL)),'ACC_GL_',''),'ACC_','')  AS ACC01_L1_MACRO_CATEGORY_CODE
                ,REPLACE(REPLACE(MAX(IFF(CONTAINS(TOP_NOD,'ACC_SC_TOT'),LVL1_NOD,NULL)),'ACC_GL_',''),'ACC_','')  AS ACC01_ACCOUNT_TYPE_CODE
                ,REPLACE(REPLACE(MAX(IFF(CONTAINS(TOP_NOD,'ACC_WAP_TOT'),TOP_NOD,NULL)),'ACC_GL_',''),'ACC_','')  AS ACC02_ACC_TOP_NOD
                ,REPLACE(REPLACE(MAX(IFF(CONTAINS(TOP_NOD,'ACC_NWAP_TOT'),TOP_NOD,NULL)),'ACC_GL_',''),'ACC_','') AS ACC03_ACC_TOP_NOD
        FROM     COP_DMT_CCD.R_CCD_ACC_UNIFY
        WHERE    ACC_ELM_COD       <> '' 
        AND      ACC_ELM_COD NOT LIKE '$%'
        GROUP BY R_CCD_ACC_UNIFY.CBU_COD
                ,R_CCD_ACC_UNIFY.ACC_ELM_COD
        HAVING   CBU_COD IN ('IBE','DCH','POL')
    )
    SELECT   CBU_COD
            ,ACC_ELM_KEY
            ,ACC_ELM_COD
            ,ACC_ELM_DSC
            ,(CASE WHEN ACC_ELM_COD = 'NA' THEN 'NA' 
                   ELSE ACC01_L5_GL_ACCOUNT_CODE 
              END)                                                       AS ACC01_L5_GL_ACCOUNT_CODE
            ,(CASE WHEN ACC_ELM_COD = 'NA' THEN 'NA' 
                   ELSE ACC01_L3_SUB_CATEGORY_CODE 
              END)                                                       AS ACC01_L3_SUB_CATEGORY_CODE
            ,(CASE WHEN ACC_ELM_COD = 'NA' THEN 'NA' 
                   ELSE ACC01_L1_MACRO_CATEGORY_CODE 
              END)                                                       AS ACC01_L1_MACRO_CATEGORY_CODE
            ,(CASE WHEN ACC_ELM_COD = 'NA' THEN 'NA' 
                   ELSE ACC01_ACCOUNT_TYPE_CODE 
              END)                                                       AS ACC01_ACCOUNT_TYPE_CODE
            ,(CASE WHEN ACC_ELM_COD = 'NA' THEN 'NA' 
                   ELSE ACC02_ACC_TOP_NOD 
              END)                                                       AS ACC02_ACC_TOP_NOD
            ,(CASE WHEN ACC_ELM_COD = 'NA' THEN 'NA' 
                   ELSE ACC03_ACC_TOP_NOD 
              END)                                                       AS ACC03_ACC_TOP_NOD
            ,0                                                           AS T_REC_DLT_FLG
            ,CURRENT_TIMESTAMP                                           AS T_REC_INS_TST
            ,CURRENT_TIMESTAMP                                           AS T_REC_UPD_TST
    FROM     DSP_ACC;

    -- Return text when the stored procedure completes

    RETURN 'Success';
EXCEPTION
    WHEN OTHER THEN
         RETURN SQLCODE || ': ' || SQLERRM;
         RAISE; -- Raise the same exception that you are handling.
END;
$$
;
