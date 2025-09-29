USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR ALTER TABLE R_FLX_CUR 
         (CUR_COD             VARCHAR(30)                             COMMENT 'Currency code'
         ,CUR_DSC             VARCHAR(500)                            COMMENT 'Currency name'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_CUR PRIMARY KEY (CUR_COD)
         ) COMMENT = '[Flex] Currency masterdata';
