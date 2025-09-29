USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TRANSIENT TABLE W_FLX_SRC_SCE__CUR 
         (CBU_COD                                 VARCHAR(10)                                              COMMENT 'CBU/Market code'
         ,ETI_ELM_COD                             VARCHAR(30)                                              COMMENT 'Entity Code'
         ,CUR_COD                                 VARCHAR(30)                                              COMMENT 'Entity currency'
         ,T_REC_UPD_TST                           TIMESTAMP_NTZ(9)
         ) DATA_RETENTION_TIME_IN_DAYS = 0 COMMENT = '[Flex] Working table used to retreive the local currency for the entity';
;
