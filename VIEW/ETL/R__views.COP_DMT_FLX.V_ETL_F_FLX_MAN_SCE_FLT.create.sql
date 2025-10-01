USE SCHEMA COP_DMT_FLX;

/*----------------------------INFO---------------------------------------------*/
/* Name : V_ETL_F_FLX_MAN_SCE_FLT                                              */
/* Date Created (dd-mm-yy): 04-04-2025                                         */
/* Owner: Flex                                                                 */
/* Modifications :                                                             */
/* 04-06-2025 MSH : T-146 - Remove *1000 from the AMOUNT column                */
/* 20-06-2025 MSH : T-183 - Added CATEGORY_ELEMENT_CODE logic with MGR         */
/* 26-06-2025 MSH : T-183 - CATEGORY_ELEMENT_CODE chg with MGR_NA, not NA_MGR  */
/* ----------------------------------------------------------------------------*/

CREATE OR REPLACE VIEW V_ETL_F_FLX_MAN_SCE_FLT
         (SCE_ELM_KEY                                                                                      COMMENT 'Flex Scenario key'
         ,CBU_COD                                                                                          COMMENT 'CBU/Market code'
         ,SCE_ELM_COD                                                                                      COMMENT 'Flex Scenario code'
         ,CFG_ORD_NUM                                                                                      COMMENT 'Order of priority (order of insertion by default)'
         ,SRC_SCE_ELM_COD                                                                                  COMMENT 'Source Scenario for the configured combination'
         ,IND_GRP_COD                                                                                      COMMENT 'Indicator code'
         ,IND_GRP_RAT_FLG                                                                                  COMMENT 'Indicator Group Ratio Flag (1 : Yes / 0 : No)'
         ,BGN_PER_ELM_COD                                                                                  COMMENT 'Filter on the Period - Beginning'
         ,END_PER_ELM_COD                                                                                  COMMENT 'Filter on the Period - End'
         ,ETI_GRP_COD                                                                                      COMMENT 'Filter in the Entity dimension'
         ,CUS_GRP_COD                                                                                      COMMENT 'Filter in the Customer dimension'
         ,CUS_DIM_GRP_COD                                                                                  COMMENT 'Customer Dimension Code (use for disagregation)'
         ,PDT_GRP_COD                                                                                      COMMENT 'Filter in the Product dimension'
         ,CAT_TYP_GRP_COD                                                                                  COMMENT 'Managerial/Interco Margin filter'
         ,EIB_GRP_COD                                                                                      COMMENT 'EIB filter'
         ,TTY_GRP_COD                                                                                      COMMENT 'Filter in the Territory dimension'
         ,SAL_SUP_GRP_COD                                                                                  COMMENT 'SU/SP filter'
         ,IND_ELM_COD                                                                                      COMMENT 'Indicator code'
         ,CFG_MOD_FLG                                                                                      COMMENT 'Allows to manage a config as negative (if -1)'
         ,IND_NUM_FLG                                                                                      COMMENT 'Indicator Numerator Flag (1 : Yes / 0 : No)'
         ,IND_DEN_FLG                                                                                      COMMENT 'Indicator Denominator Flag (1 : Yes / 0 : No)'
         ,PER_ELM_COD                                                                                      COMMENT 'Period Code (Month)'
         ,ACT_PER_FLG                                                                                      COMMENT 'Actual Period Flag'
         ,ETI_ELM_COD                                                                                      COMMENT 'Entity Element Code'
         ,CUS_ELM_COD                                                                                      COMMENT 'Customer Element Code'
         ,PDT_ELM_COD                                                                                      COMMENT 'Product Element Code'
         ,CAT_TYP_ELM_COD                                                                                  COMMENT 'Category Type Element Code'
         ,EIB_ELM_COD                                                                                      COMMENT 'EIB Element Code'
         ,TTY_ELM_COD                                                                                      COMMENT 'Territory Element Code'
         ,SAL_SUP_ELM_COD                                                                                  COMMENT 'SU/SP Element Code'
         ,SRC_CUR_COD                                                                                      COMMENT 'Source Currency'
         ,SCE_CUR_COD                                                                                      COMMENT 'Scenario input currency'
         ,CNV_CUR_RAT                                                                                      COMMENT 'Convertion currency rate'
         ,AMOUNT
         ,ACCOUNT_ELEMENT_CODE
         ,DESTINATION_ELEMENT_CODE
         ,FUNCTIONAL_AREA_ELEMENT_CODE
         ,CATEGORY_ELEMENT_CODE
         ,CHANNEL_ELEMENT_CODE
         ,IOM_CODE
         ,PLANT_CODE
         ,ORIGINAL_ACCOUNT_ELEMENT_CODE
         ,T_REC_SRC_TST
         ,T_REC_INS_TST
         ,T_REC_UPD_TST
         ) COMMENT = ''
AS
WITH FINAL AS (
    SELECT   P_FLX_SCE_CFG_IND.CBU_COD
            ,P_FLX_SCE_CFG_IND.SCE_ELM_COD
            ,P_FLX_SCE_CFG_IND.SCE_ELM_KEY
            ,P_FLX_SCE_CFG_IND.CFG_ORD_NUM
            ,P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD
            ,P_FLX_SCE_CFG_IND.IND_ELM_COD                     AS IND_GRP_COD
            ,R_FLX_GRP_IND.IND_GRP_RAT_FLG
            ,P_FLX_SCE_CFG_IND.BGN_PER_ELM_COD
            ,P_FLX_SCE_CFG_IND.END_PER_ELM_COD
            ,P_FLX_SCE_CFG_IND.ETI_ELM_COD                     AS ETI_GRP_COD
            ,P_FLX_SCE_CFG_IND.CUS_ELM_COD                     AS CUS_GRP_COD
            ,R_FLX_SCE.CUS_DIM_GRP_COD
            ,P_FLX_SCE_CFG_IND.PDT_ELM_COD                     AS PDT_GRP_COD
            ,P_FLX_SCE_CFG_IND.CAT_TYP_ELM_COD                 AS CAT_TYP_GRP_COD
            ,P_FLX_SCE_CFG_IND.EIB_ELM_COD                     AS EIB_GRP_COD
            ,P_FLX_SCE_CFG_IND.TTY_ELM_COD                     AS TTY_GRP_COD
            ,P_FLX_SCE_CFG_IND.SAL_SUP_ELM_COD                 AS SAL_SUP_GRP_COD
            ,R_FLX_GRP_IND.IND_ELM_COD                         AS IND_ELM_COD
            ,1                                                 AS CFG_MOD_FLG
            ,R_FLX_GRP_IND.IND_NUM_FLG
            ,R_FLX_GRP_IND.IND_DEN_FLG
            ,F_FLX_MAN_SCE.PER_ELM_COD                         AS PER_ELM_COD
            ,(CASE WHEN F_FLX_MAN_SCE.PER_ELM_COD <= R_FLX_SCE.LST_ACT_PER_COD THEN 1 
                   ELSE 0 
              END)                                             AS ACT_PER_FLG
            ,F_FLX_MAN_SCE.ETI_ELM_COD                         AS ETI_ELM_COD
            ,F_FLX_MAN_SCE.CUS_ELM_COD                         AS CUS_ELM_COD
            ,F_FLX_MAN_SCE.PDT_ELM_COD                         AS PDT_ELM_COD
            ,F_FLX_MAN_SCE.CAT_TYP_ELM_COD                     AS CAT_TYP_ELM_COD
            ,(CASE WHEN R_FLX_SCE.EIB_USE_FLG = 0 THEN 'NA'
                   ELSE F_FLX_MAN_SCE.EIB_ELM_COD
              END)                                             AS EIB_ELM_COD
            ,(CASE WHEN R_FLX_SCE.TTY_USE_FLG = 0 THEN 'NA'
                   ELSE F_FLX_MAN_SCE.TTY_ELM_COD
              END)                                             AS TTY_ELM_COD
            ,REGEXP_SUBSTR(F_FLX_MAN_SCE.DST_ELM_COD
                            , '_([^_-]{3}-[^_-]{3})(-[^_-]{2})?'
                            , 1, 1, 'e', 1)                    AS SAL_SUP_ELM_COD
            ,R_FLX_ETI.ETI_CUR_COD                             AS SRC_CUR_COD
            ,R_FLX_SCE.CUR_COD                                 AS SCE_CUR_COD
            ,(CASE R_FLX_SCE.CUR_COD
                   WHEN 'EUR_CY' THEN CUR.RATE_CY
                   WHEN 'EUR_FY' THEN CUR.RATE_FY
                   WHEN 'LC' THEN 1
              END)                                             AS CNV_CUR_RAT

            ,IFF(REPLACE(R_FLX_ACCOUNT.ACC_ELM_KEY,'ACC_', '') LIKE ANY ('VLM%', 'IND%')
                 -- Not really an amount, so no conversion
                ,ROUND(F_FLX_MAN_SCE.AMOUNT, 6)
                ,ROUND(F_FLX_MAN_SCE.AMOUNT * 
                       (CASE WHEN R_FLX_SCE.CUR_COD = R_FLX_MAN_SCE.MAN_SCE_CUR_COD THEN 1
                             -- Conversion to local currency
                             ELSE (CASE WHEN R_FLX_MAN_SCE.MAN_SCE_CUR_COD = 'EUR_CY' THEN CUR.RATE_CY
                                        WHEN R_FLX_MAN_SCE.MAN_SCE_CUR_COD = 'EUR_FY' THEN CUR.RATE_FY
                                        ELSE 1.0000
                                   END) 
                        END) / 
                       (CASE WHEN R_FLX_SCE.CUR_COD = R_FLX_MAN_SCE.MAN_SCE_CUR_COD THEN 1
                             -- Conversion to scenario currency
                             ELSE (CASE WHEN R_FLX_SCE.CUR_COD = 'EUR_CY' THEN CUR.RATE_CY
                                        WHEN R_FLX_SCE.CUR_COD = 'EUR_FY' THEN CUR.RATE_FY
                                        ELSE 1.0000
                                   END)
                        END)
                      , 15)
                )                                              AS AMOUNT
            
            ,F_FLX_MAN_SCE.ACC_ELM_COD                         AS ACCOUNT_ELEMENT_CODE
            ,F_FLX_MAN_SCE.DST_ELM_COD                         AS DESTINATION_ELEMENT_CODE
            ,F_FLX_MAN_SCE.FCT_ARE_ELM_COD                     AS FUNCTIONAL_AREA_ELEMENT_CODE
            ,DECODE(F_FLX_MAN_SCE.CAT_TYP_ELM_COD
                  ,'MGR','MGR_NA'
                  ,F_FLX_MAN_SCE.CAT_TYP_ELM_COD)              AS CATEGORY_ELEMENT_CODE
            ,'NA'                                              AS CHANNEL_ELEMENT_CODE
            ,'NA'                                              AS IOM_CODE
            ,'NA'                                              AS PLANT_CODE
            ,F_FLX_MAN_SCE.T_REC_UPD_TST                       AS T_REC_SRC_TST
            ,R_FLX_ACCOUNT.ACC_ELM_KEY                         AS ORIGINAL_ACCOUNT_ELEMENT_CODE
    FROM     COP_DMT_FLX.P_FLX_SCE_CFG_IND
             INNER JOIN COP_DMT_FLX.R_FLX_SCE ON 
             (
               R_FLX_SCE.SCE_ELM_KEY = P_FLX_SCE_CFG_IND.SCE_ELM_KEY
             )
             INNER JOIN COP_DMT_FLX.R_FLX_MAN_SCE ON 
             (
               R_FLX_MAN_SCE.CBU_COD         = P_FLX_SCE_CFG_IND.CBU_COD         AND
               R_FLX_MAN_SCE.MAN_SCE_ELM_COD = P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD AND
               R_FLX_MAN_SCE.MAN_SCE_USE_FLG = 1
             )
             INNER JOIN COP_DMT_FLX.F_FLX_MAN_SCE ON 
             (
               F_FLX_MAN_SCE.MAN_SCE_ELM_KEY       = R_FLX_MAN_SCE.MAN_SCE_ELM_KEY     AND
               F_FLX_MAN_SCE.PER_ELM_COD     BETWEEN P_FLX_SCE_CFG_IND.BGN_PER_ELM_COD AND P_FLX_SCE_CFG_IND.END_PER_ELM_COD
             )
             -- To retrieve the volume unit configured for the Entity
             INNER JOIN COP_DMT_FLX.R_FLX_ETI ON 
             (
                R_FLX_ETI.CBU_COD     = F_FLX_MAN_SCE.CBU_COD     AND
                R_FLX_ETI.ETI_ELM_COD = F_FLX_MAN_SCE.ETI_ELM_COD
             )
             -- Retrieve LC to EUR_CY and EUR_FY current exchange rates
             LEFT OUTER JOIN PRD_COP.COP_DSP_CONTROLLING_CLOUD.R_CURRENCY_UNIFY_AGG AS CUR ON 
             (
                 R_FLX_ETI.ETI_CUR_COD = CUR.CUR_COD AND
                 R_FLX_ETI.CBU_COD     = CUR.CBU_COD
             )
             INNER JOIN COP_DMT_FLX.R_FLX_ACCOUNT ON 
             (
                R_FLX_ACCOUNT.CBU_COD     = F_FLX_MAN_SCE.CBU_COD     AND
                R_FLX_ACCOUNT.ACC_ELM_COD = F_FLX_MAN_SCE.ACC_ELM_COD
             )
             INNER JOIN COP_DMT_FLX.R_FLX_GRP_IND ON 
             (
                R_FLX_GRP_IND.IND_GRP_COD = P_FLX_SCE_CFG_IND.IND_ELM_COD AND
                R_FLX_GRP_IND.IND_ELM_COD = F_FLX_MAN_SCE.IND_ELM_COD
             )
    WHERE    1=1
    
    AND      F_FLX_MAN_SCE.T_REC_DLT_FLG      = 0
    AND      R_FLX_SCE.INI_STS_COD       NOT IN ('created','done','failed','requested')
    
    AND      (P_FLX_SCE_CFG_IND.ETI_ELM_COD = '$TOTAL_ETI' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_ETI
                      WHERE  R_FLX_GRP_ETI.ETI_GRP_COD = P_FLX_SCE_CFG_IND.ETI_ELM_COD
                      AND    R_FLX_GRP_ETI.ETI_ELM_COD = F_FLX_MAN_SCE.ETI_ELM_COD
                     )
             )
    
    AND      (P_FLX_SCE_CFG_IND.CUS_ELM_COD = '$TOTAL_CUS' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_CUS
                      WHERE  R_FLX_GRP_CUS.CUS_GRP_COD = P_FLX_SCE_CFG_IND.CUS_ELM_COD
                      AND    R_FLX_GRP_CUS.CUS_ELM_COD = F_FLX_MAN_SCE.CUS_ELM_COD
                     )
             )
    
    AND      (P_FLX_SCE_CFG_IND.PDT_ELM_COD = '$TOTAL_PDT' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_PDT
                      WHERE  R_FLX_GRP_PDT.PDT_GRP_COD = P_FLX_SCE_CFG_IND.PDT_ELM_COD
                      AND    R_FLX_GRP_PDT.PDT_ELM_COD = F_FLX_MAN_SCE.PDT_ELM_COD
                     )
             )
    
    AND      (P_FLX_SCE_CFG_IND.CAT_TYP_ELM_COD = '$TOTAL_CAT_TYP' 
    OR        EXISTS (SELECT 1
                      FROM COP_DMT_FLX.R_FLX_GRP_CAT_TYP
                      WHERE R_FLX_GRP_CAT_TYP.CAT_TYP_GRP_COD = P_FLX_SCE_CFG_IND.CAT_TYP_ELM_COD
                      AND   R_FLX_GRP_CAT_TYP.CAT_TYP_ELM_COD = F_FLX_MAN_SCE.CAT_TYP_ELM_COD
                      )
             )
    AND      (P_FLX_SCE_CFG_IND.EIB_ELM_COD = '$TOTAL_EIB' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_EIB
                      WHERE  R_FLX_GRP_EIB.EIB_GRP_COD = P_FLX_SCE_CFG_IND.EIB_ELM_COD
                      AND    R_FLX_GRP_EIB.EIB_ELM_COD = F_FLX_MAN_SCE.EIB_ELM_COD
                     )
             )
    
    AND      (P_FLX_SCE_CFG_IND.TTY_ELM_COD = '$TOTAL_TTY' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_TTY
                      WHERE  R_FLX_GRP_TTY.TTY_GRP_COD = P_FLX_SCE_CFG_IND.TTY_ELM_COD
                      AND    R_FLX_GRP_TTY.TTY_ELM_COD = F_FLX_MAN_SCE.TTY_ELM_COD
                     )
             )
    
)
SELECT   SCE_ELM_KEY
        ,CBU_COD
        ,SCE_ELM_COD
        ,ANY_VALUE(CFG_ORD_NUM)                             AS CFG_ORD_NUM
        ,ANY_VALUE(SRC_SCE_ELM_COD)                         AS SRC_SCE_ELM_COD
        ,IND_GRP_COD
        ,ANY_VALUE(IND_GRP_RAT_FLG)                         AS IND_GRP_RAT_FLG
        ,BGN_PER_ELM_COD
        ,END_PER_ELM_COD
        ,ETI_GRP_COD
        ,CUS_GRP_COD
        ,CUS_DIM_GRP_COD
        ,PDT_GRP_COD
        ,CAT_TYP_GRP_COD
        ,EIB_GRP_COD
        ,TTY_GRP_COD
        ,SAL_SUP_GRP_COD
        ,IND_ELM_COD
        ,CFG_MOD_FLG
        ,ANY_VALUE(IND_NUM_FLG)                             AS IND_NUM_FLG
        ,ANY_VALUE(IND_DEN_FLG)                             AS IND_DEN_FLG
        ,PER_ELM_COD
        ,ANY_VALUE(ACT_PER_FLG)                             AS ACT_PER_FLG
        ,ETI_ELM_COD
        ,CUS_ELM_COD
        ,PDT_ELM_COD
        ,CAT_TYP_ELM_COD
        ,EIB_ELM_COD
        ,TTY_ELM_COD
        ,SAL_SUP_ELM_COD
        ,SRC_CUR_COD
        ,ANY_VALUE(SCE_CUR_COD)                             AS SCE_CUR_COD
        ,ANY_VALUE(CNV_CUR_RAT)                             AS CNV_CUR_RAT
        ,SUM(AMOUNT)                                        AS AMOUNT
        ,ACCOUNT_ELEMENT_CODE
        ,DESTINATION_ELEMENT_CODE
        ,FUNCTIONAL_AREA_ELEMENT_CODE
        -- These dimensions are not useful for the disaggregation
        ,ANY_VALUE(CATEGORY_ELEMENT_CODE)                   AS CATEGORY_ELEMENT_CODE
        ,ANY_VALUE(CHANNEL_ELEMENT_CODE)                    AS CHANNEL_ELEMENT_CODE
        ,ANY_VALUE(IOM_CODE)                                AS IOM_CODE
        ,ANY_VALUE(PLANT_CODE)                              AS PLANT_CODE
        ,ORIGINAL_ACCOUNT_ELEMENT_CODE
        ,TO_TIMESTAMP_NTZ(MAX(T_REC_SRC_TST))               AS T_REC_SRC_TST
        ,CURRENT_TIMESTAMP                                  AS T_REC_INS_TST
        ,CURRENT_TIMESTAMP                                  AS T_REC_UPD_TST
FROM     FINAL
GROUP BY ALL
;
