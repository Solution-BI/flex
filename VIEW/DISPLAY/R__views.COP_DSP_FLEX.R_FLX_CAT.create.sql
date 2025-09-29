USE SCHEMA COP_DSP_FLEX{{uid}};

CREATE OR REPLACE VIEW R_FLX_CAT 
         (CAT_ELM_KEY                                                 COMMENT 'Category Key : concatenation of CBU and Category Code'
         ,CAT_ELM_COD                                                 COMMENT 'Category code'
         ,CAT_ELM_DSC                                                 COMMENT 'Category name'
         ,CBU_COD                                                     COMMENT 'CBU/Market'
         ,CAT_TYP_COD                                                 COMMENT 'Managerial/L500/...'
         ) COMMENT = 'Category masterdata'
AS
SELECT    CAT_ELM_KEY
         ,CAT_ELM_COD
         ,CAT_ELM_DSC
         ,CBU_COD    
         ,CAT_TYP_COD
FROM      COP_DMT_FLX.R_FLX_CAT
;
