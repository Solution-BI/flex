USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW P_FLX_ETI_PDT_FCA
         (ID                                                                                    COMMENT 'ID for writeback'
         ,CBU_COD                                                                               COMMENT 'CBU/Market'
         ,CFG_ORD_NUM                                                                           COMMENT 'Order of priority (order of insertion by default)'
         ,ETI_ELM_COD                                                                           COMMENT 'Entity code'
         ,ETI_ELM_DSC                                                                           COMMENT 'Entity name'
         ,PDT_GRP_COD                                                                           COMMENT 'Product group'
         ,FCA_MAT_OTH_VAL                                                                       COMMENT 'FCA amount for the Rest of Material Costs (in LC)'
         ,FCA_MANUF_OTH_VAL                                                                     COMMENT 'FCA amount for the Rest of Manuf. Costs (in LC)'
         ,FCA_LOG_OTH_VAL                                                                       COMMENT 'FCA amount for the Rest of Log. Costs (in LC)'
         ) COMMENT = '[Flex] Flex parameter table for the COGS FCA'
AS
SELECT    ID
         ,CBU_COD
         ,CFG_ORD_NUM
         ,ETI_ELM_COD
         ,ETI_ELM_DSC
         ,PDT_GRP_COD
         ,FCA_MAT_OTH_VAL
         ,FCA_MANUF_OTH_VAL
         ,FCA_LOG_OTH_VAL
FROM      COP_DMT_FLX.P_FLX_ETI_PDT_FCA
;
