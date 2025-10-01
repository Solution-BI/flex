USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE VIEW V_ETL_R_FLX_SCE_DLT
         (CBU_COD                                                           COMMENT 'CBU/Market'
         ,SCE_ELM_COD                                                       COMMENT 'Scenario code (generated automatically)'
         ,SCE_ELM_KEY                                                       COMMENT 'Scenario Key : concatenation of CBU and Scenario Code'
         ,DLT_RQT_TST                                                       COMMENT 'Date and time of the deletion request'
         ) COMMENT = '[Flex] Flex scenario to delete in SQLServer'
AS
SELECT   CBU_COD
        ,SCE_ELM_COD
        ,SCE_ELM_KEY
        ,DLT_RQT_TST
FROM     COP_DMT_FLX.R_FLX_SCE
WHERE    DLT_STS_COD = 'requested'
;
