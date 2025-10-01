USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW R_FLX_SAL_SUP 
         (SAL_SUP_ELM_KEY                                       COMMENT 'Sales Supply Point Key : concatenation of CBU and Sales Supply Point Code'
         ,SAL_SUP_ELM_COD                                       COMMENT 'Sales Supply Point code'
         ,SAL_SUP_ELM_DSC                                       COMMENT 'Sales Supply Point name'
         ,CBU_COD                                               COMMENT 'CBU/Market'
         ) COMMENT = '[Flex] Sales Supply Point masterdata'
AS
SELECT    SAL_SUP_ELM_KEY
         ,SAL_SUP_ELM_COD
         ,SAL_SUP_ELM_DSC
         ,CBU_COD    
FROM      COP_DMT_FLX.R_FLX_SAL_SUP
;
