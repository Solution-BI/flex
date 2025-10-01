USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE R_FLX_SAL_SUP 
         (SAL_SUP_ELM_KEY     VARCHAR(64)                             COMMENT 'Sales Supply Point Key : concatenation of CBU and Sales Supply Point Code'
         ,SAL_SUP_ELM_COD     VARCHAR(30)                             COMMENT 'Sales Supply Point code'
         ,SAL_SUP_ELM_DSC     VARCHAR(500)                            COMMENT 'Sales Supply Point name'
         ,CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_SAL_SUP PRIMARY KEY (SAL_SUP_ELM_KEY)
         ) COMMENT = '[Flex] Sales Supply Point masterdata';

DELETE FROM R_FLX_SAL_SUP;

INSERT INTO R_FLX_SAL_SUP
      (SAL_SUP_ELM_KEY
      ,SAL_SUP_ELM_COD
      ,SAL_SUP_ELM_DSC
      ,CBU_COD
      ,T_REC_DLT_FLG
      ,T_REC_INS_TST
      ,T_REC_UPD_TST
      )
VALUES ('DCH-SU','SU','Sales Unit','DCH',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
      ,('DCH-SP','SP','Supply Point','DCH',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
      ,('DCH-NA','NA','N/A','DCH',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
      ,('IBE-SU','SU','Sales Unit','IBE',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
      ,('IBE-SP','SP','Supply Point','IBE',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
      ,('IBE-NA','NA','N/A','IBE',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
      ;
