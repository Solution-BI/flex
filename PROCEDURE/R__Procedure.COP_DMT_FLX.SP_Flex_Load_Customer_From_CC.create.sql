USE DATABASE {{env}}_COP;
USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Customer_From_CC() 
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_CUS
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 01-07-2024                                                 
=========================================================================
Modified On:      Description:                        Author:          
2024/09/17        Add the CUS_DIM_GRP_COD 1 & 2       N. COQUIO
=========================================================================
*/
DECLARE
v_STEP_TABLE     VARCHAR(256);
BEGIN

    -- Load the table R_FLX_CUS in frp mode

    v_STEP_TABLE := 'R_FLX_CUS';

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_CUS;

    INSERT INTO COP_DMT_FLX.R_FLX_CUS
          (CUS_ELM_KEY
          ,CUS_ELM_COD
          ,CUS_ELM_DSC
          ,CBU_COD
          ,LV1_CUS_COD
          ,LV1_CUS_DSC
          ,LV2_CUS_COD
          ,LV2_CUS_DSC
          ,LV3_CUS_COD
          ,LV3_CUS_DSC
          ,LV4_CUS_COD
          ,LV4_CUS_DSC
          ,LV5_CUS_COD
          ,LV5_CUS_DSC
          ,LV6_CUS_COD
          ,LV6_CUS_DSC
          ,T_REC_DLT_FLG
          ,T_REC_INS_TST
          ,T_REC_UPD_TST
          )
    SELECT CBU_COD || '-' || CUSTOMER_CODE                                       CUS_ELM_KEY
          ,CUSTOMER_CODE                                                         CUS_ELM_COD
          ,CUSTOMER                                                              CUS_ELM_DSC
          ,CBU_COD                                                               CBU_COD
          ,REPLACE(L1_CUSTOMER_DISTRIBUTION_CHANNEL_CODE,'CUS_')                 LV1_CUS_COD
          ,L1_CUSTOMER_DISTRIBUTION_CHANNEL_DESC                                 LV1_CUS_DSC
          ,REPLACE(L2_CUSTOMER_DISTRIBUTION_CHANNEL_MARKET_CODE,'CUS_')          LV2_CUS_COD
          ,L2_CUSTOMER_DISTRIBUTION_CHANNEL_MARKET_DESC                          LV2_CUS_DSC
          ,REPLACE(L3_CUSTOMER_CODE,'CUS_')                                      LV3_CUS_COD
          ,L3_CUSTOMER_DESC                                                      LV3_CUS_DSC
          ,REPLACE(L4_CUSTOMER_CODE,'CUS_')                                      LV4_CUS_COD
          ,L4_CUSTOMER_DESC                                                      LV4_CUS_DSC
          ,REPLACE(L5_CUSTOMER_CODE,'CUS_')                                      LV5_CUS_COD
          ,L5_CUSTOMER_DESC                                                      LV5_CUS_DSC
          ,'NA'                                                                  LV6_CUS_COD
          ,'Non Applicable'                                                      LV6_CUS_DSC
          ,0                                                                     T_REC_DLT_FLG
          ,CURRENT_TIMESTAMP                                                     T_REC_INS_TST
          ,CURRENT_TIMESTAMP                                                     T_REC_UPD_TST
    FROM   PRD_COP.COP_DSP_CONTROLLING_CLOUD.R_CUSTOMER_UNIFY
     -- Exclude FR customers (legacy data that is not purged from Tagetik, should not be necessary after the retrofit)
    WHERE NOT (
      CBU_COD = 'DCH' 
      AND REPLACE(L2_CUSTOMER_DISTRIBUTION_CHANNEL_MARKET_CODE,'CUS_', '') LIKE 'CTL_L2_FRA%'
    )
    ;

    -- Insert a NA_ERR Customer in Flex masterdata:
    -- it exists in the CC facts but not in the CC masterdata.
    INSERT INTO COP_DMT_FLX.R_FLX_CUS
    SELECT CBU_COD ||'-NA_ERR'  AS CUS_ELM_KEY
          ,'NA_ERR'             AS CUS_ELM_COD
          ,'NA_ERR'             AS CUS_ELM_DSC
          ,CBU_COD
          ,'NA'                 AS LV1_CUS_COD
          ,'Non Applicable'     AS LV1_CUS_DSC
          ,'NA'                 AS LV2_CUS_COD
          ,'Non Applicable'     AS LV2_CUS_DSC
          ,'NA'                 AS LV3_CUS_COD
          ,'Non Applicable'     AS LV3_CUS_DSC
          ,'NA'                 AS LV4_CUS_COD
          ,'Non Applicable'     AS LV4_CUS_DSC
          ,'NA'                 AS LV5_CUS_COD
          ,'Non Applicable'     AS LV5_CUS_DSC
          ,'NA'                 AS LV6_CUS_COD
          ,'Non Applicable'     AS LV6_CUS_DSC
          ,0                    AS T_REC_DLT_FLG
          ,CURRENT_TIMESTAMP    AS T_REC_INS_TST
          ,CURRENT_TIMESTAMP    AS T_REC_UPD_TST
    FROM  ( -- Only insert if no NA_ERR Customer arrives from the source
           SELECT DISTINCT CBU_COD FROM COP_DMT_FLX.R_FLX_CUS
           MINUS
           SELECT DISTINCT CBU_COD FROM COP_DMT_FLX.R_FLX_CUS WHERE CUS_ELM_COD = 'NA_ERR'
          );

    -- Insert missing customer in Flex masterdata from the source scenario
    INSERT INTO COP_DMT_FLX.R_FLX_CUS
    SELECT DISTINCT
           full_.CBU_COD || '-' || full_.CUSTOMER_ELEMENT_CODE                   CUS_ELM_KEY
          ,full_.CUSTOMER_ELEMENT_CODE                                           CUS_ELM_COD
          ,'Not mapped'                                                          CUS_ELM_DSC
          ,full_.CBU_COD                                                         CBU_COD
          ,'NM'                                                                  LV1_CUS_COD
          ,'Not mapped'                                                          LV1_CUS_DSC
          ,'NM'                                                                  LV2_CUS_COD
          ,'Not mapped'                                                          LV2_CUS_DSC
          ,'NM'                                                                  LV3_CUS_COD
          ,'Not mapped'                                                          LV3_CUS_DSC
          ,'NM'                                                                  LV4_CUS_COD
          ,'Not mapped'                                                          LV4_CUS_DSC
          ,'NM'                                                                  LV5_CUS_COD
          ,'Not mapped'                                                          LV5_CUS_DSC
          ,'NA'                                                                  LV6_CUS_COD
          ,'Non Applicable'                                                      LV6_CUS_DSC
          ,0                                                                     T_REC_DLT_FLG
          ,CURRENT_TIMESTAMP                                                     T_REC_INS_TST
          ,CURRENT_TIMESTAMP                                                     T_REC_UPD_TST
    FROM   PRD_COP.COP_DSP_CONTROLLING_CLOUD.F_CCD_FULL_DTL_UNIFY full_
           LEFT OUTER JOIN COP_DMT_FLX.R_FLX_CUS as cus ON
           (
                cus.CBU_COD     = full_.CBU_COD               AND
                cus.CUS_ELM_COD = full_.CUSTOMER_ELEMENT_CODE
           )
    WHERE  cus.CUS_ELM_KEY IS NULL;

    v_STEP_TABLE := 'R_FLX_GRP_CUS';

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_GRP_CUS;

    INSERT OVERWRITE ALL 
        INTO COP_DMT_FLX.R_FLX_GRP_CUS(CBU_COD,CUS_ELM_COD,CUS_DIM_GRP_COD,CUS_GRP_COD,CUS_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
             VALUES (CBU_COD, CUS_ELM_COD, -1, '$TOTAL_CUS', 'Total Customer', T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        INTO COP_DMT_FLX.R_FLX_GRP_CUS(CBU_COD,CUS_ELM_COD,CUS_DIM_GRP_COD,CUS_GRP_COD,CUS_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
             VALUES (CBU_COD, CUS_ELM_COD,  0, CUS_ELM_COD, CUS_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        INTO COP_DMT_FLX.R_FLX_GRP_CUS(CBU_COD,CUS_ELM_COD,CUS_DIM_GRP_COD,CUS_GRP_COD,CUS_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
             VALUES (CBU_COD, CUS_ELM_COD,  1, LV1_CUS_COD, LV1_CUS_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        INTO COP_DMT_FLX.R_FLX_GRP_CUS(CBU_COD,CUS_ELM_COD,CUS_DIM_GRP_COD,CUS_GRP_COD,CUS_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
             VALUES (CBU_COD, CUS_ELM_COD,  2, LV2_CUS_COD, LV2_CUS_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        INTO COP_DMT_FLX.R_FLX_GRP_CUS(CBU_COD,CUS_ELM_COD,CUS_DIM_GRP_COD,CUS_GRP_COD,CUS_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
             VALUES (CBU_COD, CUS_ELM_COD,  3, LV3_CUS_COD, LV3_CUS_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        INTO COP_DMT_FLX.R_FLX_GRP_CUS(CBU_COD,CUS_ELM_COD,CUS_DIM_GRP_COD,CUS_GRP_COD,CUS_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
             VALUES (CBU_COD, CUS_ELM_COD,  4, LV4_CUS_COD, LV4_CUS_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
        INTO COP_DMT_FLX.R_FLX_GRP_CUS(CBU_COD,CUS_ELM_COD,CUS_DIM_GRP_COD,CUS_GRP_COD,CUS_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
             VALUES (CBU_COD, CUS_ELM_COD,  5, LV5_CUS_COD, LV5_CUS_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)

    SELECT   cus.CBU_COD
            ,cus.CUS_ELM_COD
            ,cus.CUS_ELM_DSC
   
            ,cus.LV1_CUS_COD
            ,cus.LV1_CUS_DSC
            ,cus.LV2_CUS_COD
            ,cus.LV2_CUS_DSC
            ,cus.LV3_CUS_COD
            ,cus.LV3_CUS_DSC
            ,cus.LV4_CUS_COD
            ,cus.LV4_CUS_DSC
            ,cus.LV5_CUS_COD
            ,cus.LV5_CUS_DSC
       
            ,0                 AS T_REC_DLT_FLG
            ,current_timestamp as T_REC_INS_TST
            ,current_timestamp as T_REC_UPD_TST
    FROM     COP_DMT_FLX.R_FLX_CUS as cus
    ORDER BY CBU_COD, CUS_ELM_COD;

    -- Return text when the stored procedure completes
    RETURN 'Success';

EXCEPTION
    WHEN OTHER THEN
         RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
         RAISE; -- Raise the same exception that you are handling.
END;
$$
;
