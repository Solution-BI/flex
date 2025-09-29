USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_ETI_PDT_FCA
         (CBU_COD                                 VARCHAR(10)                                              COMMENT 'CBU/Market code'
         ,ETI_ELM_COD                             VARCHAR(30)                                              COMMENT 'Entity Code'
         ,PDT_ELM_COD                             VARCHAR(30)                                              COMMENT 'Product Code'
         ,CFG_ORD_NUM                             NUMBER(2,0)                                              COMMENT 'Maximum Order of priority'
         ,FCA_MAT_OTH_VAL                         NUMBER(32,12)                                            COMMENT 'FCA amount for the Rest of Material Costs (in LC)'
         ,FCA_MANUF_OTH_VAL                       NUMBER(32,12) DEFAULT 0                                  COMMENT 'FCA amount for the Rest of Manuf. Costs (in LC)'
         ,FCA_LOG_OTH_VAL                         NUMBER(32,12) DEFAULT 0                                  COMMENT 'FCA amount for the Rest of Log. Costs (in LC)'
         ,T_REC_DLT_FLG                           NUMBER(2,0)                                              COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST                           TIMESTAMP_TZ                                             COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST                           TIMESTAMP_TZ                                             COMMENT '[Technical] Timestamp of last update into the table'
         ) COMMENT = '[Flex] Referentiel table for the FCA per Market/Entity and Product (loaded from the table P_FLX_ETI_PDT_FCA)';
