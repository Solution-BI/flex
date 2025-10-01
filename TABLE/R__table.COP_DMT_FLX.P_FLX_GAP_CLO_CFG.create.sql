USE SCHEMA COP_DMT_FLX;

CREATE SEQUENCE IF NOT EXISTS SEQ__P_FLX_GAP_CLO_CFG__ID;

CREATE OR REPLACE TABLE P_FLX_GAP_CLO_CFG (
          ID                                      NUMBER(38)     DEFAULT SEQ__P_FLX_SCE_CFG_IND__ID.NEXTVAL                         COMMENT 'ID for writeback'
         ,SCE_ELM_KEY                             VARCHAR(64)                                                                       COMMENT 'Flex Scenario Key'
         ,CBU_COD                                 VARCHAR(10)                                                                       COMMENT 'CBU/Market code'
         ,SCE_ELM_COD                             VARCHAR(50)                                                                       COMMENT 'Flex Scenario Code'
         ,ETI_ELM_COD                             VARCHAR(30)                                                                       COMMENT 'Entity code'
         ,LV0_PDT_CAT_COD                         VARCHAR(30)                                                                       COMMENT 'Product Category code'
         ,GAP_CLO_PER_COD                         VARCHAR(30)                                                                       COMMENT 'Config the Period for closing the gap'
         ,T_REC_DLT_FLG                           NUMBER(2)                                                                         COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST                           TIMESTAMP_TZ(9)                                                                   COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST                           TIMESTAMP_TZ(9)                                                                   COMMENT '[Technical] Timestamp of last update into the table'
)COMMENT = '[Flex] Configuration of scenarios for closing the gap';