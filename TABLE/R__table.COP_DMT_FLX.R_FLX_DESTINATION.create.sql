USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_DESTINATION
         (CBU_COD                      VARCHAR(10)                             COMMENT 'CBU/Market'
         ,DST_ELM_KEY                  VARCHAR(50)                             COMMENT 'Original Destination code'
         ,DST_ELM_COD                  VARCHAR(50)                             COMMENT 'Destination code'
         ,DST_ELM_DSC                  VARCHAR(500)                            COMMENT 'Destination name'
         ,DST01_L3_DESTINATION_CODE    VARCHAR(50)
         ,DST01_L2_DESTINATION_CODE    VARCHAR(50)
         ,DST01_L1_DESTINATION_CODE    VARCHAR(50)
         ,T_REC_DLT_FLG                NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST                TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST                TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_CAT PRIMARY KEY (CBU_COD,DST_ELM_KEY)
         ) COMMENT = 'Controling Cloud Destination masterdata';
