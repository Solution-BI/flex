USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE VIEW V_ETL_W_FLX_SRC_SCE_FLT
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
            ,W_FLX_SRC_SCE.CFG_MOD_FLG
            ,R_FLX_GRP_IND.IND_NUM_FLG
            ,R_FLX_GRP_IND.IND_DEN_FLG
            ,W_FLX_SRC_SCE.PERIOD_ELEMENT_CODE                 AS PER_ELM_COD
            ,(CASE WHEN W_FLX_SRC_SCE.PERIOD_ELEMENT_CODE <= R_FLX_SCE.LST_ACT_PER_COD THEN 1 
                   ELSE 0 
              END)                                             AS ACT_PER_FLG
            ,W_FLX_SRC_SCE.ENTITY_ELEMENT_CODE                 AS ETI_ELM_COD
            ,W_FLX_SRC_SCE.CUSTOMER_ELEMENT_CODE               AS CUS_ELM_COD
            ,W_FLX_SRC_SCE.PRODUCT_ELEMENT_CODE                AS PDT_ELM_COD
            ,W_FLX_SRC_SCE.MGR_L500_CODE                       AS CAT_TYP_ELM_COD
            ,(CASE WHEN R_FLX_SCE.EIB_USE_FLG = 0 THEN 'NON_EIB'
                   ELSE W_FLX_SRC_SCE.EIB_ELEMENT_CODE
              END)                                             AS EIB_ELM_COD
            ,(CASE WHEN R_FLX_SCE.TTY_USE_FLG = 0 THEN 'NA'
                   ELSE W_FLX_SRC_SCE.TERRITORY_ELEMENT_CODE
              END)                                             AS TTY_ELM_COD
            ,W_FLX_SRC_SCE.SU_SP_SPLIT_CODE                    AS SAL_SUP_ELM_COD
            ,W_FLX_SRC_SCE.CUR_COD                             AS SRC_CUR_COD
            ,R_FLX_SCE.CUR_COD                                 AS SCE_CUR_COD
            ,(CASE R_FLX_SCE.CUR_COD
                   WHEN 'EUR_CY' THEN W_FLX_SRC_SCE.RATE_CY
                   WHEN 'EUR_FY' THEN W_FLX_SRC_SCE.RATE_FY
                   WHEN 'LC' THEN 1
              END)                                             AS CNV_CUR_RAT
        
            ,(CASE R_FLX_SCE.CUR_COD
                   WHEN 'EUR_CY' THEN W_FLX_SRC_SCE.AMOUNT_EUR_CY
                   WHEN 'EUR_FY' THEN W_FLX_SRC_SCE.AMOUNT_EUR_FY
                   WHEN 'LC' THEN W_FLX_SRC_SCE.AMOUNT_LC
              END) * W_FLX_SRC_SCE.CFG_MOD_FLG                 AS AMOUNT
            
            ,W_FLX_SRC_SCE.ACCOUNT_ELEMENT_CODE
            ,W_FLX_SRC_SCE.DESTINATION_ELEMENT_CODE
            ,W_FLX_SRC_SCE.FUNCTIONAL_AREA_ELEMENT_CODE
            ,W_FLX_SRC_SCE.CATEGORY_ELEMENT_CODE
            ,W_FLX_SRC_SCE.CHANNEL_ELEMENT_CODE
            ,W_FLX_SRC_SCE.IOM_CODE
            ,W_FLX_SRC_SCE.PLANT_CODE
            ,W_FLX_SRC_SCE.T_REC_SRC_TST
            ,W_FLX_SRC_SCE.ORIGINAL_ACCOUNT_ELEMENT_CODE
    FROM     COP_DMT_FLX.P_FLX_SCE_CFG_IND
             INNER JOIN COP_DMT_FLX.R_FLX_SCE ON 
             (
               R_FLX_SCE.SCE_ELM_KEY = P_FLX_SCE_CFG_IND.SCE_ELM_KEY
             )
             INNER JOIN W_FLX_SRC_SCE ON 
             (
               P_FLX_SCE_CFG_IND.CBU_COD               = W_FLX_SRC_SCE.CBU_COD               AND
               P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD       = W_FLX_SRC_SCE.SCENARIO_ELEMENT_CODE AND
               W_FLX_SRC_SCE.PERIOD_ELEMENT_CODE BETWEEN P_FLX_SCE_CFG_IND.BGN_PER_ELM_COD   AND P_FLX_SCE_CFG_IND.END_PER_ELM_COD
             )
             -- To retrieve the volume unit configured for the Entity
             INNER JOIN COP_DMT_FLX.R_FLX_ETI ON 
             (
                R_FLX_ETI.CBU_COD     = W_FLX_SRC_SCE.CBU_COD             AND
                R_FLX_ETI.ETI_ELM_COD = W_FLX_SRC_SCE.ENTITY_ELEMENT_CODE
             )
             INNER JOIN COP_DMT_FLX.R_FLX_GRP_IND ON 
             (
                R_FLX_GRP_IND.IND_GRP_COD = P_FLX_SCE_CFG_IND.IND_ELM_COD AND
                R_FLX_GRP_IND.IND_ELM_COD = W_FLX_SRC_SCE.INDICATOR_CODE
                -- Retrieve the Volume in the unit configured for the Entity no more needed in UNIFY
                --(CASE WHEN R_FLX_GRP_IND.IND_ELM_COD = 'VOL' THEN 
                --            W_FLX_SRC_SCE.INDICATOR_CODE = 'VOL_'|| R_FLX_ETI.VOL_UNT_COD 
                --      ELSE R_FLX_GRP_IND.IND_ELM_COD = W_FLX_SRC_SCE.INDICATOR_CODE
                --  END)
             )
    WHERE    1=1
    
    AND      R_FLX_SCE.INI_STS_COD NOT IN ('created','done','failed','requested')
    
    AND      (P_FLX_SCE_CFG_IND.ETI_ELM_COD = '$TOTAL_ETI' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_ETI
                      WHERE  R_FLX_GRP_ETI.ETI_GRP_COD = P_FLX_SCE_CFG_IND.ETI_ELM_COD
                      AND    R_FLX_GRP_ETI.ETI_ELM_COD = W_FLX_SRC_SCE.ENTITY_ELEMENT_CODE
                     )
             )
    
    AND      (P_FLX_SCE_CFG_IND.CUS_ELM_COD = '$TOTAL_CUS' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_CUS
                      WHERE  R_FLX_GRP_CUS.CUS_GRP_COD = P_FLX_SCE_CFG_IND.CUS_ELM_COD
                      AND    R_FLX_GRP_CUS.CUS_ELM_COD = W_FLX_SRC_SCE.CUSTOMER_ELEMENT_CODE
                     )
             )
    
    AND      (P_FLX_SCE_CFG_IND.PDT_ELM_COD = '$TOTAL_PDT' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_PDT
                      WHERE  R_FLX_GRP_PDT.PDT_GRP_COD = P_FLX_SCE_CFG_IND.PDT_ELM_COD
                      AND    R_FLX_GRP_PDT.PDT_ELM_COD = W_FLX_SRC_SCE.PRODUCT_ELEMENT_CODE
                     )
             )
    
    AND      (P_FLX_SCE_CFG_IND.CAT_TYP_ELM_COD = '$TOTAL_CAT_TYP' 
    OR        EXISTS (SELECT 1
                      FROM COP_DMT_FLX.R_FLX_GRP_CAT_TYP
                      WHERE R_FLX_GRP_CAT_TYP.CAT_TYP_GRP_COD = P_FLX_SCE_CFG_IND.CAT_TYP_ELM_COD
                      AND   R_FLX_GRP_CAT_TYP.CAT_TYP_ELM_COD = W_FLX_SRC_SCE.MGR_L500_CODE
                      )
             )
    AND      (P_FLX_SCE_CFG_IND.EIB_ELM_COD = '$TOTAL_EIB' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_EIB
                      WHERE  R_FLX_GRP_EIB.EIB_GRP_COD = P_FLX_SCE_CFG_IND.EIB_ELM_COD
                      AND    R_FLX_GRP_EIB.EIB_ELM_COD = W_FLX_SRC_SCE.EIB_ELEMENT_CODE
                     )
             )
    
    AND      (P_FLX_SCE_CFG_IND.TTY_ELM_COD = '$TOTAL_TTY' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_TTY
                      WHERE  R_FLX_GRP_TTY.TTY_GRP_COD = P_FLX_SCE_CFG_IND.TTY_ELM_COD
                      AND    R_FLX_GRP_TTY.TTY_ELM_COD = W_FLX_SRC_SCE.TERRITORY_ELEMENT_CODE
                     )
             )
    
    AND      (P_FLX_SCE_CFG_IND.SAL_SUP_ELM_COD = '$TOTAL_SAL_SUP' 
    OR        EXISTS (SELECT 1
                      FROM   COP_DMT_FLX.R_FLX_GRP_SAL_SUP
                      WHERE  R_FLX_GRP_SAL_SUP.SAL_SUP_GRP_COD = P_FLX_SCE_CFG_IND.SAL_SUP_ELM_COD
                      AND    R_FLX_GRP_SAL_SUP.SAL_SUP_ELM_COD = W_FLX_SRC_SCE.SU_SP_SPLIT_CODE
                     )
             )
    
    -- Only keep the latest config for each record
    QUALIFY  P_FLX_SCE_CFG_IND.CFG_ORD_NUM = MAX(P_FLX_SCE_CFG_IND.CFG_ORD_NUM) 
                                             OVER (PARTITION BY P_FLX_SCE_CFG_IND.CBU_COD
                                                               ,P_FLX_SCE_CFG_IND.SCE_ELM_COD
                                                               ,P_FLX_SCE_CFG_IND.SCE_ELM_KEY
             -- For indicators, we want to keep only one version for the indicators that will be used in the target scenario
             -- but we want to keep all the versions for denominators used to calculate ratios (they will be discarded after calculation).
                                                               ,R_FLX_GRP_IND.IND_ELM_COD
                                                               ,(CASE WHEN IND_NUM_FLG = 0 THEN IND_GRP_COD 
                                                                      ELSE 'ONLY_LATEST' 
                                                                 END)
                                                               ,W_FLX_SRC_SCE.PERIOD_ELEMENT_CODE
                                                               ,W_FLX_SRC_SCE.ENTITY_ELEMENT_CODE
                                                               ,W_FLX_SRC_SCE.CUSTOMER_ELEMENT_CODE
                                                               ,W_FLX_SRC_SCE.PRODUCT_ELEMENT_CODE
                                                               ,W_FLX_SRC_SCE.MGR_L500_CODE
                                                               ,(CASE WHEN R_FLX_SCE.EIB_USE_FLG = 0 THEN 'NON_EIB'
                                                                      ELSE W_FLX_SRC_SCE.EIB_ELEMENT_CODE
                                                                 END)
                                                               ,(CASE WHEN R_FLX_SCE.TTY_USE_FLG = 0 THEN 'NA'
                                                                      ELSE W_FLX_SRC_SCE.TERRITORY_ELEMENT_CODE
                                                                 END)
                                                               ,W_FLX_SRC_SCE.SU_SP_SPLIT_CODE
                                                               )
)
SELECT   SCE_ELM_KEY
        ,CBU_COD
        ,SCE_ELM_COD
        ,ANY_VALUE(CFG_ORD_NUM)           AS CFG_ORD_NUM
        ,ANY_VALUE(SRC_SCE_ELM_COD)       AS SRC_SCE_ELM_COD
        ,IND_GRP_COD
        ,ANY_VALUE(IND_GRP_RAT_FLG)       AS IND_GRP_RAT_FLG
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
        ,ANY_VALUE(IND_NUM_FLG)           AS IND_NUM_FLG
        ,ANY_VALUE(IND_DEN_FLG)           AS IND_DEN_FLG
        ,PER_ELM_COD
        ,ANY_VALUE(ACT_PER_FLG)           AS ACT_PER_FLG
        ,ETI_ELM_COD
        ,CUS_ELM_COD
        ,PDT_ELM_COD
        ,CAT_TYP_ELM_COD
        ,EIB_ELM_COD
        ,TTY_ELM_COD
        ,SAL_SUP_ELM_COD
        ,SRC_CUR_COD
        ,ANY_VALUE(SCE_CUR_COD)           AS SCE_CUR_COD
        ,ANY_VALUE(CNV_CUR_RAT)           AS CNV_CUR_RAT
        ,SUM(AMOUNT)                      AS AMOUNT
        ,ACCOUNT_ELEMENT_CODE
        ,DESTINATION_ELEMENT_CODE
        ,FUNCTIONAL_AREA_ELEMENT_CODE
        -- These dimensions are not useful for the disaggregation
        ,ANY_VALUE(CATEGORY_ELEMENT_CODE) AS CATEGORY_ELEMENT_CODE
        ,ANY_VALUE(CHANNEL_ELEMENT_CODE)  AS CHANNEL_ELEMENT_CODE
        ,ANY_VALUE(IOM_CODE)              AS IOM_CODE
        ,ANY_VALUE(PLANT_CODE)            AS PLANT_CODE
        ,ORIGINAL_ACCOUNT_ELEMENT_CODE
        ,MAX(T_REC_SRC_TST)               AS T_REC_SRC_TST
        ,CURRENT_TIMESTAMP                AS T_REC_INS_TST
        ,CURRENT_TIMESTAMP                AS T_REC_UPD_TST
FROM     FINAL
GROUP BY ALL
