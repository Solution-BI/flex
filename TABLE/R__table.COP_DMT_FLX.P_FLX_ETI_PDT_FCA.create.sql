USE SCHEMA COP_DMT_FLX{{uid}};

CREATE SEQUENCE IF NOT EXISTS P_FLX_ETI_PDT_FCA_seq;

CREATE OR REPLACE TABLE P_FLX_ETI_PDT_FCA 
         (ID                          NUMBER        DEFAULT P_FLX_ETI_PDT_FCA_seq.NEXTVAL       COMMENT 'ID for writeback'
         ,CBU_COD                     VARCHAR(10)                                               COMMENT 'CBU/Market'
         ,CFG_ORD_NUM                 NUMBER(2)                                                 COMMENT 'Order of priority (order of insertion by default)'
         ,ETI_ELM_COD                 VARCHAR(30)                                               COMMENT 'Entity code'
         ,ETI_ELM_DSC                 VARCHAR(500)                                              COMMENT 'Entity name'
         ,PDT_GRP_COD                 VARCHAR(30)                                               COMMENT 'Product group'
         ,FCA_MAT_OTH_VAL             NUMBER(32,12)                                             COMMENT 'FCA amount for the Rest of Material Costs (in LC)'
         ,FCA_MANUF_OTH_VAL           NUMBER(32,12) DEFAULT 0                                   COMMENT 'FCA amount for the Rest of Manuf. Costs (in LC)'
         ,FCA_LOG_OTH_VAL             NUMBER(32,12) DEFAULT 0                                   COMMENT 'FCA amount for the Rest of Log. Costs (in LC)'
         ,T_REC_DLT_FLG               NUMBER(2,0)                                               COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST               TIMESTAMP_TZ                                              COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST               TIMESTAMP_TZ                                              COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_P_FLX_ETI_PDT_FCA PRIMARY KEY (ID)
         ) COMMENT = '[Flex] Flex parameter table for the COGS FCA';