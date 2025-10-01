USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW R_FLX_CUS 
         (CUS_ELM_KEY                                             COMMENT 'Customer Key : concatenation of CBU and Customer Code'
         ,CUS_ELM_COD                                             COMMENT 'Customer code'
         ,CUS_ELM_DSC                                             COMMENT 'Customer name'
         ,CBU_COD                                                 COMMENT 'CBU/Market'
         ,LV1_CUS_COD                                             COMMENT 'L1 Customer code (C hierarchy)'
         ,LV1_CUS_DSC                                             COMMENT 'L1 Customer name (C hierarchy)'
         ,LV2_CUS_COD                                             COMMENT 'L2 Customer code (C hierarchy)'
         ,LV2_CUS_DSC                                             COMMENT 'L2 Customer name (C hierarchy)'
         ,LV3_CUS_COD                                             COMMENT 'L3 Customer code (C hierarchy)'
         ,LV3_CUS_DSC                                             COMMENT 'L3 Customer name (C hierarchy)'
         ,LV4_CUS_COD                                             COMMENT 'L4 Customer code (C hierarchy)'
         ,LV4_CUS_DSC                                             COMMENT 'L4 Customer name (C hierarchy)'
         ,LV5_CUS_COD                                             COMMENT 'L5 Customer code (C hierarchy)'
         ,LV5_CUS_DSC                                             COMMENT 'L5 Customer name (C hierarchy)'
         ,LV6_CUS_COD                                             COMMENT 'L6 Customer code (C hierarchy)'
         ,LV6_CUS_DSC                                             COMMENT 'L6 Customer name (C hierarchy)'
         ) COMMENT = '[Flex] Customer masterdata'
AS
WITH ALL_LEVEL AS (
SELECT    CUS_ELM_KEY
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
         ,CAST(6 AS SMALLINT)           LVL_COD
FROM      COP_DMT_FLX.R_FLX_CUS
UNION ALL
SELECT    DISTINCT
          CBU_COD || '-' || LV5_CUS_COD CUS_ELM_KEY
         ,LV5_CUS_COD                   CUS_ELM_COD
         ,LV5_CUS_DSC                   CUS_ELM_DSC
         ,CBU_COD                       CBU_COD
         ,LV1_CUS_COD                   LV1_CUS_COD
         ,LV1_CUS_DSC                   LV1_CUS_DSC
         ,LV2_CUS_COD                   LV2_CUS_COD
         ,LV2_CUS_DSC                   LV2_CUS_DSC
         ,LV3_CUS_COD                   LV3_CUS_COD
         ,LV3_CUS_DSC                   LV3_CUS_DSC
         ,LV4_CUS_COD                   LV4_CUS_COD
         ,LV4_CUS_DSC                   LV4_CUS_DSC
         ,LV5_CUS_COD                   LV5_CUS_COD
         ,LV5_CUS_DSC                   LV5_CUS_DSC
         ,LV5_CUS_COD                   LV6_CUS_COD
         ,LV5_CUS_DSC                   LV6_CUS_DSC
         ,CAST(5 AS SMALLINT)           LVL_COD
FROM      COP_DMT_FLX.R_FLX_CUS
UNION ALL
SELECT    DISTINCT
          CBU_COD || '-' || LV4_CUS_COD CUS_ELM_KEY
         ,LV4_CUS_COD                   CUS_ELM_COD
         ,LV4_CUS_DSC                   CUS_ELM_DSC
         ,CBU_COD                       CBU_COD
         ,LV1_CUS_COD                   LV1_CUS_COD
         ,LV1_CUS_DSC                   LV1_CUS_DSC
         ,LV2_CUS_COD                   LV2_CUS_COD
         ,LV2_CUS_DSC                   LV2_CUS_DSC
         ,LV3_CUS_COD                   LV3_CUS_COD
         ,LV3_CUS_DSC                   LV3_CUS_DSC
         ,LV4_CUS_COD                   LV4_CUS_COD
         ,LV4_CUS_DSC                   LV4_CUS_DSC
         ,LV4_CUS_COD                   LV5_CUS_COD
         ,LV4_CUS_DSC                   LV5_CUS_DSC
         ,LV4_CUS_COD                   LV6_CUS_COD
         ,LV4_CUS_DSC                   LV6_CUS_DSC
         ,CAST(4 AS SMALLINT)           LVL_COD
FROM      COP_DMT_FLX.R_FLX_CUS
UNION ALL
SELECT    DISTINCT
          CBU_COD || '-' || LV3_CUS_COD CUS_ELM_KEY
         ,LV3_CUS_COD                   CUS_ELM_COD
         ,LV3_CUS_DSC                   CUS_ELM_DSC
         ,CBU_COD                       CBU_COD
         ,LV1_CUS_COD                   LV1_CUS_COD
         ,LV1_CUS_DSC                   LV1_CUS_DSC
         ,LV2_CUS_COD                   LV2_CUS_COD
         ,LV2_CUS_DSC                   LV2_CUS_DSC
         ,LV3_CUS_COD                   LV3_CUS_COD
         ,LV3_CUS_DSC                   LV3_CUS_DSC
         ,LV3_CUS_COD                   LV4_CUS_COD
         ,LV3_CUS_DSC                   LV4_CUS_DSC
         ,LV3_CUS_COD                   LV5_CUS_COD
         ,LV3_CUS_DSC                   LV5_CUS_DSC
         ,LV3_CUS_COD                   LV6_CUS_COD
         ,LV3_CUS_DSC                   LV6_CUS_DSC
         ,CAST(3 AS SMALLINT)           LVL_COD
FROM      COP_DMT_FLX.R_FLX_CUS
-- Add Level 1 & 2 NCOQ 2024/09/10
UNION ALL
SELECT    DISTINCT
          CBU_COD || '-' || LV2_CUS_COD CUS_ELM_KEY
         ,LV2_CUS_COD                   CUS_ELM_COD
         ,LV2_CUS_DSC                   CUS_ELM_DSC
         ,CBU_COD                       CBU_COD
         ,LV1_CUS_COD                   LV1_CUS_COD
         ,LV1_CUS_DSC                   LV1_CUS_DSC
         ,LV2_CUS_COD                   LV2_CUS_COD
         ,LV2_CUS_DSC                   LV2_CUS_DSC
         ,LV2_CUS_COD                   LV3_CUS_COD
         ,LV2_CUS_DSC                   LV3_CUS_DSC
         ,LV2_CUS_COD                   LV4_CUS_COD
         ,LV2_CUS_DSC                   LV4_CUS_DSC
         ,LV2_CUS_COD                   LV5_CUS_COD
         ,LV2_CUS_DSC                   LV5_CUS_DSC
         ,LV2_CUS_COD                   LV6_CUS_COD
         ,LV2_CUS_DSC                   LV6_CUS_DSC
         ,CAST(2 AS SMALLINT)           LVL_COD
FROM      COP_DMT_FLX.R_FLX_CUS
UNION ALL
SELECT    DISTINCT
          CBU_COD || '-' || LV1_CUS_COD CUS_ELM_KEY
         ,LV1_CUS_COD                   CUS_ELM_COD
         ,LV1_CUS_DSC                   CUS_ELM_DSC
         ,CBU_COD                       CBU_COD
         ,LV1_CUS_COD                   LV1_CUS_COD
         ,LV1_CUS_DSC                   LV1_CUS_DSC
         ,LV1_CUS_COD                   LV2_CUS_COD
         ,LV1_CUS_DSC                   LV2_CUS_DSC
         ,LV1_CUS_COD                   LV3_CUS_COD
         ,LV1_CUS_DSC                   LV3_CUS_DSC
         ,LV1_CUS_COD                   LV4_CUS_COD
         ,LV1_CUS_DSC                   LV4_CUS_DSC
         ,LV1_CUS_COD                   LV5_CUS_COD
         ,LV1_CUS_DSC                   LV5_CUS_DSC
         ,LV1_CUS_COD                   LV6_CUS_COD
         ,LV1_CUS_DSC                   LV6_CUS_DSC
         ,CAST(1 AS SMALLINT)           LVL_COD
FROM      COP_DMT_FLX.R_FLX_CUS
)
,data_ as (
-- Add the rank based on the min Level Code and the Customer level code to ensure no duplicate key
SELECT    CUS_ELM_KEY
         ,CUS_ELM_COD
         ,CUS_ELM_DSC
         ,ALL_LEVEL.CBU_COD
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
         ,LVL_COD
         ,RANK() OVER (PARTITION BY CUS_ELM_KEY
                       ORDER BY LVL_COD
                               ,LV6_CUS_COD
                               ,LV5_CUS_COD
                               ,LV4_CUS_COD
                               ,LV3_CUS_COD
                               ,LV2_CUS_COD
                               ,LV1_CUS_COD)        RNK_MIN_LVL
FROM      ALL_LEVEL
)
SELECT    CUS_ELM_KEY
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
FROM      data_
WHERE     RNK_MIN_LVL = 1
;
