USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_EIB 
         (EIB_ELM_KEY         VARCHAR(64)                          COMMENT 'Business type (EIB) Key : concatenation of CBU and Business type Code'
         ,EIB_ELM_COD         VARCHAR(30)                          COMMENT 'Business type (EIB) code'
         ,EIB_ELM_DSC         VARCHAR(500)                         COMMENT 'Business type (EIB) name'
         ,CBU_COD             VARCHAR(10)                          COMMENT 'CBU/Market'
         ,T_REC_DLT_FLG       NUMBER(2,0)                          COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST       TIMESTAMP_TZ                         COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                         COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_EIB PRIMARY KEY (EIB_ELM_KEY)
         ) COMMENT = '[Flex] Business type (EIB) masterdata';
