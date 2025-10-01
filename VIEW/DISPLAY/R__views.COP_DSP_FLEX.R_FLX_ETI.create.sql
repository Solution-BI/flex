USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW R_FLX_ETI 
         (ETI_ELM_KEY                                                 COMMENT 'Entity Key : concatenation of CBU and Entity Code'
         ,ETI_ELM_COD                                                 COMMENT 'Entity code'
         ,ETI_ELM_DSC                                                 COMMENT 'Entity name'
         ,CBU_COD                                                     COMMENT 'CBU/Market'
         ,ETI_CRY_COD                                                 COMMENT 'Entity Country code'
         ,ETI_CRY_DSC                                                 COMMENT 'Entity Country name'
         ) COMMENT = '[Flex] Entity masterdata'
AS
SELECT    ETI_ELM_KEY
         ,ETI_ELM_COD
         ,ETI_ELM_DSC
         ,CBU_COD
         ,ETI_CRY_COD
         ,ETI_CRY_DSC
FROM      COP_DMT_FLX.R_FLX_ETI
WHERE     T_REC_DLT_FLG = 0
;
