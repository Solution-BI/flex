USE SCHEMA COP_DSP_FLEX{{uid}};

CREATE OR REPLACE VIEW R_FLX_PER 
         (PER_ELM_COD                                                   COMMENT 'Period code'
         ,PER_ELM_DSC                                                   COMMENT 'Period name'
         ,QOY_ELM_COD                                                   COMMENT 'Quarter of year code'
         ,QOY_ELM_DSC                                                   COMMENT 'Quarter of year name'
         ,SOY_ELM_COD                                                   COMMENT 'Semester of year code'
         ,SOY_ELM_DSC                                                   COMMENT 'Semester of year name'
         ,FYR_ELM_COD                                                   COMMENT 'Full year code'
         ,FYR_ELM_DSC                                                   COMMENT 'Full year name'
         ) COMMENT = '[Flex] Period masterdata'
AS
SELECT    PER_ELM_COD
         ,PER_ELM_DSC
         ,QOY_ELM_COD
         ,QOY_ELM_DSC
         ,SOY_ELM_COD
         ,SOY_ELM_DSC
         ,FYR_ELM_COD
         ,FYR_ELM_DSC
FROM      COP_DMT_FLX.R_FLX_PER
;
