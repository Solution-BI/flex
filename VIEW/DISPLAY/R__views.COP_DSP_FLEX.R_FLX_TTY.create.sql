USE SCHEMA COP_DSP_FLEX{{uid}};

CREATE OR REPLACE VIEW R_FLX_TTY 
         (TTY_ELM_KEY                                                 COMMENT 'Territory Key : concatenation of CBU and Territory Code'
         ,TTY_ELM_COD                                                 COMMENT 'Territory code'
         ,TTY_ELM_DSC                                                 COMMENT 'Territory name'
         ,CBU_COD                                                     COMMENT 'CBU/Market'
         ) COMMENT = '[Flex] Territory masterdata'
AS
SELECT    TTY_ELM_KEY
         ,TTY_ELM_COD
         ,TTY_ELM_DSC
         ,CBU_COD
FROM      COP_DMT_FLX.R_FLX_TTY
;
