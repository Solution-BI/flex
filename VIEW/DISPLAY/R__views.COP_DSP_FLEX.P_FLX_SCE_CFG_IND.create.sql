USE SCHEMA COP_DSP_FLEX{{uid}};

CREATE OR REPLACE VIEW P_FLX_SCE_CFG_IND
         (ID                                                      COMMENT 'ID for writeback'
         ,SCE_ELM_KEY                                             COMMENT 'Flex Scenario key'
         ,CBU_COD                                                 COMMENT 'CBU/Market code'
         ,SCE_ELM_COD                                             COMMENT 'Flex Scenario code'
         ,IND_ELM_COD                                             COMMENT 'Indicator code'
         ,CFG_ORD_NUM                                             COMMENT 'Order of priority (order of insertion by default)'
         ,BGN_PER_ELM_COD                                         COMMENT 'Filter on the Period - Beginning'
         ,END_PER_ELM_COD                                         COMMENT 'Filter on the Period - End'
         ,ETI_ELM_COD                                             COMMENT 'Filter in the Entity dimension'
         ,CUS_ELM_COD                                             COMMENT 'Filter in the Customer dimension'
         ,PDT_ELM_COD                                             COMMENT 'Filter in the Product dimension'
         ,CAT_TYP_ELM_COD                                         COMMENT 'Managerial/Interco Margin filter'
         ,EIB_ELM_COD                                             COMMENT 'EIB filter'
         ,TTY_ELM_COD                                             COMMENT 'Filter in the Territory dimension'
         ,SAL_SUP_ELM_COD                                         COMMENT 'SU/SP filter'
         ,SRC_SCE_ELM_COD                                         COMMENT 'Source Scenario for the configured combination'
         ) COMMENT = '[Flex] Configuration of source scenarios by KPI'
AS
SELECT    ID
         ,SCE_ELM_KEY
         ,CBU_COD
         ,SCE_ELM_COD
         ,IND_ELM_COD
         ,CFG_ORD_NUM
         ,BGN_PER_ELM_COD
         ,END_PER_ELM_COD
         ,ETI_ELM_COD
         ,CUS_ELM_COD
         ,PDT_ELM_COD
         ,CAT_TYP_ELM_COD
         ,EIB_ELM_COD
         ,TTY_ELM_COD
         ,SAL_SUP_ELM_COD
         ,SRC_SCE_ELM_COD
FROM      COP_DMT_FLX.P_FLX_SCE_CFG_IND
WHERE     T_REC_DLT_FLG = 0
;
