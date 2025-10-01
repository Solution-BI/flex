USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_SAL_SUP_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
========================================================================================
                                HISTORY
========================================================================================
Description : Create Procedure Script to load data in R_FLX_SAL_SUP
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 27-06-2024                                                 
========================================================================================
Modified On:      Description:                                    Author:          
30-05-2025        T-115 - add segment info: su_sp_split_code      SHUDDHO Manan (SBI)
========================================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);

BEGIN

    -- Load the table R_FLX_EIB in frp mode

    v_STEP_TABLE := 'R_FLX_SAL_SUP';

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_SAL_SUP;

    INSERT INTO COP_DMT_FLX.R_FLX_SAL_SUP
          (SAL_SUP_ELM_KEY
          ,SAL_SUP_ELM_COD
          ,SAL_SUP_ELM_DSC
          ,CBU_COD
          ,T_REC_DLT_FLG
          ,T_REC_INS_TST
          ,T_REC_UPD_TST
          )
    WITH _DATA_ (SAL_SUP_ELM_COD,SAL_SUP_ELM_DSC) AS (
    /*** BEGIN DATA ***/
      SELECT DISTINCT 
            COALESCE(regexp_substr(DST_KEY_COD, '_([^_-]{3}-[^_-]{3})(-[^_-]{2})?', 1, 1, 'e', 1), 'NA') as SAL_SUP_ELM_COD,
            COALESCE(regexp_substr(DST_KEY_COD, '_([^_-]{3}-[^_-]{3})(-[^_-]{2})?', 1, 1, 'e', 1), 'N/A') as SAL_SUP_ELM_DSC
      FROM PRD_COP.COP_DSP_CONTROLLING_CLOUD.R_DESTINATION_UNIFY
    /*** END DATA ***/
    )
    SELECT CBU_COD || '-' || SAL_SUP_ELM_COD       SAL_SUP_ELM_KEY
          ,SAL_SUP_ELM_COD                         SAL_SUP_ELM_COD
          ,SAL_SUP_ELM_DSC                         SAL_SUP_ELM_DSC
          ,CBU_COD                                 CBU_COD
          ,0                                       T_REC_DLT_FLG
          ,CURRENT_TIMESTAMP                       T_REC_INS_TST
          ,CURRENT_TIMESTAMP                       T_REC_UPD_TST
    FROM   COP_DMT_FLX.R_FLX_CBU
          ,_DATA_;

    v_STEP_TABLE := 'R_FLX_GRP_SAL_SUP';

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_GRP_SAL_SUP;


    INSERT OVERWRITE ALL
    INTO COP_DMT_FLX.R_FLX_GRP_SAL_SUP
         (CBU_COD, SAL_SUP_ELM_COD, SAL_SUP_DIM_GRP_COD, SAL_SUP_GRP_COD, SAL_SUP_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
         VALUES (CBU_COD, SAL_SUP_ELM_COD, -1, '$TOTAL_SAL_SUP', 'Total Organisation & Segment', T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
    INTO COP_DMT_FLX.R_FLX_GRP_SAL_SUP
         (CBU_COD, SAL_SUP_ELM_COD, SAL_SUP_DIM_GRP_COD, SAL_SUP_GRP_COD, SAL_SUP_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
         VALUES (CBU_COD, SAL_SUP_ELM_COD,  0, SAL_SUP_ELM_COD, SAL_SUP_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
    SELECT   CBU_COD
            ,SAL_SUP_ELM_COD
            ,SAL_SUP_ELM_DSC
            ,0                                           as T_REC_DLT_FLG
            ,current_timestamp                           as T_REC_INS_TST
            ,current_timestamp                           as T_REC_UPD_TST
    FROM     COP_DMT_FLX.R_FLX_SAL_SUP
    ORDER BY CBU_COD, SAL_SUP_ELM_COD;

    -- Return text when the stored procedure completes

    RETURN 'Success';
EXCEPTION
    WHEN OTHER THEN
         RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
         RAISE; -- Raise the same exception that you are handling.
END;
$$
;
