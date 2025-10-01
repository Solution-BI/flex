USE SCHEMA COP_DMT_FLX;

CREATE SEQUENCE IF NOT EXISTS SEQ__P_FLX_SCE_CFG_IND__ID;

CREATE OR REPLACE TABLE P_FLX_SCE_CFG_IND 
      (ID                  NUMBER        DEFAULT SEQ__P_FLX_SCE_CFG_IND__ID.NEXTVAL COMMENT 'ID for writeback'
      ,SCE_ELM_KEY         VARCHAR(64)                                              COMMENT 'Flex Scenario key'
      ,CBU_COD             VARCHAR(10)                                              COMMENT 'CBU/Market code'
      ,SCE_ELM_COD         VARCHAR(50)                                              COMMENT 'Flex Scenario code'
      ,IND_ELM_COD         VARCHAR(30)                                              COMMENT 'Indicator code'
      ,CFG_ORD_NUM         NUMBER(2,0)                                              COMMENT 'Order of priority (order of insertion by default)'

--      ,UPD_SGY_COD         NUMBER(2,0)                                              COMMENT 'Update strategy: Upsert (1: DEFAULT) or Del/Ins (2) on the scope'

      ,BGN_PER_ELM_COD     VARCHAR(30)                                              COMMENT 'Filter on the Period - Beginning'
      ,END_PER_ELM_COD     VARCHAR(30)                                              COMMENT 'Filter on the Period - End'
      ,ETI_ELM_COD         VARCHAR(30)                                              COMMENT 'Filter in the Entity dimension'
      ,CUS_ELM_COD         VARCHAR(30)                                              COMMENT 'Filter in the Customer dimension'
      ,PDT_ELM_COD         VARCHAR(30)                                              COMMENT 'Filter in the Product dimension'
      ,CAT_TYP_ELM_COD     VARCHAR(30)                                              COMMENT 'Managerial/Interco Margin filter'
      ,EIB_ELM_COD         VARCHAR(30)                                              COMMENT 'EIB filter'
      ,TTY_ELM_COD         VARCHAR(30)                                              COMMENT 'Filter in the Territory dimension'
      ,SAL_SUP_ELM_COD     VARCHAR(30)                                              COMMENT 'SU/SP filter'

      ,SRC_SCE_ELM_COD     VARCHAR(30)                                              COMMENT 'Source Scenario for the configured combination'

      ,T_REC_DLT_FLG       NUMBER(2,0)                                              COMMENT '[Technical] Physical deletion flag'
      ,T_REC_INS_TST       TIMESTAMP_TZ                                             COMMENT '[Technical] Timestamp of first insertion into the table'
      ,T_REC_UPD_TST       TIMESTAMP_TZ                                             COMMENT '[Technical] Timestamp of last update into the table'
         
      ,CONSTRAINT PK_P_FLX_SCE_CFG_IND PRIMARY KEY (ID)
         
      ) COMMENT = '[Flex] Configuration of source scenarios by KPI'
;