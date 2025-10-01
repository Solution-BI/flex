USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE VIEW V_ETL_R_FLX_SCE_CCD
         (CBU_COD                                                           COMMENT 'CBU/Market'
         ,SCE_ELM_COD                                                       COMMENT 'Scenario code (generated automatically)'
         ,SCE_ELM_KEY                                                       COMMENT 'Scenario Key : concatenation of CBU and Scenario Code'
         ,CCD_RQT_TST                                                       COMMENT 'Date and time of the request to copy to Controlling Cloud'
         ) COMMENT = '[Flex] Flex scenario to copy to CCD'
AS
SELECT   CBU_COD
        ,SCE_ELM_COD
        ,SCE_ELM_KEY
        ,CCD_RQT_TST
FROM     COP_DMT_FLX.R_FLX_SCE
WHERE    CCD_STS_COD = 'requested'
;
