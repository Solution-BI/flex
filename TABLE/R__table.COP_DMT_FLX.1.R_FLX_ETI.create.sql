USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_ETI 
         (ETI_ELM_KEY         VARCHAR(64)                             COMMENT 'Entity Key : concatenation of CBU and Entity Code'
         ,ETI_ELM_COD         VARCHAR(30)                             COMMENT 'Entity code'
         ,ETI_ELM_DSC         VARCHAR(500)                            COMMENT 'Entity name'
         ,CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market'
         ,ETI_CRY_COD         VARCHAR(30)                             COMMENT 'Entity Country code'
         ,ETI_CRY_DSC         VARCHAR(300)                            COMMENT 'Entity Country name'
         ,ETI_CUR_COD         VARCHAR(7)                              COMMENT 'currency code (Manage manually)'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_ETI PRIMARY KEY (ETI_ELM_KEY)
) COMMENT = '[Flex] Entity masterdata';
