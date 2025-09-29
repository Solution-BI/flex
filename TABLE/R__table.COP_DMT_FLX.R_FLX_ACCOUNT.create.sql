USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_ACCOUNT 
         (CBU_COD                         VARCHAR(10)                             COMMENT 'CBU/Market'
         ,ACC_ELM_KEY                     VARCHAR(50)                             COMMENT 'Original Account code'
         ,ACC_ELM_COD                     VARCHAR(30)                             COMMENT 'Account code'
         ,ACC_ELM_DSC                     VARCHAR(500)                            COMMENT 'Account name'
         ,ACC01_L5_GL_ACCOUNT_CODE        VARCHAR(50)
         ,ACC01_L3_SUB_CATEGORY_CODE      VARCHAR(50)
         ,ACC01_L1_MACRO_CATEGORY_CODE    VARCHAR(50)
         ,ACC01_ACCOUNT_TYPE_CODE         VARCHAR(50)
         ,ACC02_ACC_TOP_NOD               VARCHAR(50)
         ,ACC03_ACC_TOP_NOD               VARCHAR(50)
         ,T_REC_DLT_FLG                   NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST                   TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST                   TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_ACCOUNT PRIMARY KEY (CBU_COD,ACC_ELM_KEY)
         ) COMMENT = 'Controling Cloud Account masterdata';
