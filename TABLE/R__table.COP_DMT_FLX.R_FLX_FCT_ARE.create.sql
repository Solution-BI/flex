USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE R_FLX_FCT_ARE 
         (CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market'
         ,FCT_ARE_ELM_COD     VARCHAR(50)                             COMMENT 'Functional Area code'
         ,FCT_ARE_ELM_DSC     VARCHAR(500)                            COMMENT 'Functional Area name'
         ,LVL1_NOD            VARCHAR(50)
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_FCT_ARE PRIMARY KEY (CBU_COD,FCT_ARE_ELM_COD)
         ) COMMENT = 'Controling Cloud Functional Area masterdata';
