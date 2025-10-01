USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE R_FLX_CAT 
         (CAT_ELM_KEY         VARCHAR(64)                             COMMENT 'Category Key : concatenation of CBU and Category Code'
         ,CAT_ELM_COD         VARCHAR(30)                             COMMENT 'Category code'
         ,CAT_ELM_DSC         VARCHAR(500)                            COMMENT 'Category name'
         ,CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market'
         ,CAT_TYP_COD         VARCHAR(30)                             COMMENT 'Managerial/L500/...'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_CAT PRIMARY KEY (CAT_ELM_KEY)
         ) COMMENT = 'Category masterdata';

DELETE FROM R_FLX_CAT;

INSERT INTO R_FLX_CAT
      (CAT_ELM_KEY
      ,CAT_ELM_COD
      ,CAT_ELM_DSC
      ,CBU_COD
      ,CAT_TYP_COD
      ,T_REC_DLT_FLG
      ,T_REC_INS_TST
      ,T_REC_UPD_TST
      )
VALUES ('DCH-NA','NA','NA','DCH','NA',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
      ,('IBE-NA','NA','NA','IBE','NA',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
;
