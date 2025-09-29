USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_TTY 
         (TTY_ELM_KEY         VARCHAR(64)                             COMMENT 'Territory Key : concatenation of CBU and Territory Code'
         ,TTY_ELM_COD         VARCHAR(30)                             COMMENT 'Territory code'
         ,TTY_ELM_DSC         VARCHAR(500)                            COMMENT 'Territory name'
         ,CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_TTY PRIMARY KEY (TTY_ELM_COD)
         ) COMMENT = '[Flex] Territory masterdata';
