USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW R_FLX_CAT_TYP 
         (CAT_TYP_ELM_KEY                                             COMMENT 'Category Type Key : concatenation of CBU and Category Type Code'
         ,CAT_TYP_ELM_COD                                             COMMENT 'Category Type code'
         ,CAT_TYP_ELM_DSC                                             COMMENT 'Category Type name'
         ,CBU_COD                                                     COMMENT 'CBU/Market'
         ) COMMENT = 'Category Type masterdata'
AS
SELECT    CAT_TYP_ELM_KEY
         ,CAT_TYP_ELM_COD
         ,CAT_TYP_ELM_DSC
         ,CBU_COD
FROM      COP_DMT_FLX.R_FLX_CAT_TYP
;
