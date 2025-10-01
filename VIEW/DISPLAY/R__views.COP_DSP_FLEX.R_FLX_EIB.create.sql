USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW R_FLX_EIB 
         (EIB_ELM_KEY                                      COMMENT 'Business type (EIB) Key : concatenation of CBU and Business type Code'
         ,EIB_ELM_COD                                      COMMENT 'Business type (EIB) code'
         ,EIB_ELM_DSC                                      COMMENT 'Business type (EIB) name'
         ,CBU_COD                                          COMMENT 'CBU/Market'
         ) COMMENT = '[Flex] Business type (EIB) masterdata'
AS
SELECT    EIB_ELM_KEY
         ,EIB_ELM_COD
         ,EIB_ELM_DSC
         ,CBU_COD
FROM      COP_DMT_FLX.R_FLX_EIB
;
