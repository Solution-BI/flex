USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW R_FLX_SRC_SCE 
         (SRC_SCE_ELM_KEY                     COMMENT 'Source scenario key concatenation of the cbu code and the scenario code'
         ,SRC_SCE_ELM_COD                     COMMENT 'Source scenario code'
         ,SRC_SCE_ELM_DSC                     COMMENT 'Source scenario description'
         ,CBU_COD                             COMMENT 'CBU/Market'
         ) COMMENT = '[Flex] Source scenario masterdata (from Controlling Cloud or Flex or Flat Files)'
AS
-- SELECT    R_SCENARIO_LIST_UNIFY.CBU_COD       || '-' || 
--           R_SCENARIO_LIST_UNIFY.SCENARIO_CODE                  SRC_SCE_ELM_KEY
--          ,R_SCENARIO_LIST_UNIFY.SCENARIO_CODE                  SRC_SCE_ELM_COD
--          ,R_SCENARIO_LIST_UNIFY.SCENARIO_CODE                  SRC_SCE_ELM_DSC
--          ,R_SCENARIO_LIST_UNIFY.CBU_COD                        CBU_COD
-- FROM      {{database}}.COP_DSP_CONTROLLING_CLOUD.R_SCENARIO_LIST_UNIFY
-- UNION
SELECT    SCE_ELM_KEY                                   SRC_SCE_ELM_KEY
         ,SCE_ELM_COD                                   SRC_SCE_ELM_COD
         ,SCE_ELM_DSC || ' [Flex]'                      SRC_SCE_ELM_DSC
         ,CBU_COD
FROM      COP_DMT_FLX.R_FLX_SCE
WHERE     R_FLX_SCE.INI_STS_COD        = 'done'
AND       R_FLX_SCE.CCD_STS_COD       != 'done'
AND       R_FLX_SCE.DLT_STS_COD       != 'done'
UNION
SELECT    MAN_SCE_ELM_KEY                                   SRC_SCE_ELM_KEY
         ,MAN_SCE_ELM_COD                                   SRC_SCE_ELM_COD
         ,MAN_SCE_ELM_DSC || ' [Manual]'                    SRC_SCE_ELM_DSC
         ,CBU_COD
FROM      COP_DMT_FLX.R_FLX_MAN_SCE
WHERE     R_FLX_MAN_SCE.MAN_SCE_USE_FLG = 1
AND       R_FLX_MAN_SCE.MAN_SCE_DLT_FLG = 0
ORDER BY  (CASE WHEN SRC_SCE_ELM_DSC LIKE '%Manual%' THEN 2
                WHEN SRC_SCE_ELM_DSC LIKE '%Flex%'   THEN 1
                ELSE 0
           END)
;
