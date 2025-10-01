USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW P_FLX_GAP_CLO_CFG
         (ID                                                      COMMENT 'ID for writeback'
         ,SCE_ELM_KEY                                             COMMENT 'Flex Scenario Key'
         ,CBU_COD                                                 COMMENT 'CBU/Market code'
         ,SCE_ELM_COD                                             COMMENT 'Flex Scenario Code'
         ,ETI_ELM_COD                                             COMMENT 'Entity code'
         ,LV0_PDT_CAT_COD                                         COMMENT 'Product Category code'
         ,GAP_CLO_PER_COD                                         COMMENT 'Config the Period for closing the gap'
         ) COMMENT = '[Flex] Configuration of scenarios for closing the gap'
AS
SELECT    ID
         ,SCE_ELM_KEY
         ,CBU_COD
         ,SCE_ELM_COD
         ,ETI_ELM_COD
         ,LV0_PDT_CAT_COD
         ,GAP_CLO_PER_COD
FROM      COP_DMT_FLX.P_FLX_GAP_CLO_CFG
WHERE     T_REC_DLT_FLG = 0
;
