USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_PER 
         (PER_ELM_COD         VARCHAR(30)                               COMMENT 'Period code'
         ,PER_ELM_DSC         VARCHAR(500)                              COMMENT 'Period name'
         ,QOY_ELM_COD         VARCHAR(30)                               COMMENT 'Quarter of year code'
         ,QOY_ELM_DSC         VARCHAR(500)                              COMMENT 'Quarter of year name'
         ,SOY_ELM_COD         VARCHAR(30)                               COMMENT 'Semester of year code'
         ,SOY_ELM_DSC         VARCHAR(500)                              COMMENT 'Semester of year name'
         ,FYR_ELM_COD         VARCHAR(30)                               COMMENT 'Full year code'
         ,FYR_ELM_DSC         VARCHAR(500)                              COMMENT 'Full year name'
         ,T_REC_DLT_FLG       NUMBER(2,0)                               COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST       TIMESTAMP_TZ                              COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                              COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_PER PRIMARY KEY (PER_ELM_COD)
         ) COMMENT = '[Flex] Period masterdata';
