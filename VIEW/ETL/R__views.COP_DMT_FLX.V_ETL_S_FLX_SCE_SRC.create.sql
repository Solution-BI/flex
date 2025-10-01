USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE VIEW V_ETL_S_FLX_SCE_SRC
         (SRC_CBU_COD                                                       COMMENT 'Source CBU/Market'
         ,SRC_SCE_ELM_COD                                                   COMMENT 'Source Scenario code (generated automatically)'
         ,SRC_SCE_ELM_KEY                                                   COMMENT 'Source Scenario Key : concatenation of CBU and Scenario Code'
         ,SCE_ELM_KEY                                                       COMMENT 'Scenario Key : concatenation of CBU and Scenario Code'
         ) COMMENT = '[Flex] Flex scenario to source from SQLServer'
AS
SELECT  R_FLX_SCE_SRC.CBU_COD        SRC_CBU_COD
       ,R_FLX_SCE_SRC.SCE_ELM_COD    SRC_SCE_ELM_COD
       ,R_FLX_SCE_SRC.SCE_ELM_KEY    SRC_SCE_ELM_KEY
       ,R_FLX_SCE.SCE_ELM_KEY          
FROM    COP_DMT_FLX.R_FLX_SCE
        INNER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON
        (
           R_FLX_SCE.SCE_ELM_KEY = P_FLX_SCE_CFG_IND.SCE_ELM_KEY
        )
        INNER JOIN COP_DMT_FLX.R_FLX_SCE R_FLX_SCE_SRC ON
        (
           R_FLX_SCE_SRC.CBU_COD      = P_FLX_SCE_CFG_IND.CBU_COD         AND
           R_FLX_SCE_SRC.SCE_ELM_COD  = P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD AND
           R_FLX_SCE_SRC.INI_STS_COD  = 'done'                            AND
           R_FLX_SCE_SRC.CCD_STS_COD != 'done'                            AND
           R_FLX_SCE_SRC.DLT_STS_COD != 'done'
        )
WHERE   R_FLX_SCE.INI_STS_COD NOT IN ('created','requested','done','failed')
;
