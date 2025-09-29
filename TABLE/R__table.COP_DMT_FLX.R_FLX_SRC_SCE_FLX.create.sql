USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_SRC_SCE_FLX 
         (SRC_SCE_ELM_COD             VARCHAR(30)                     COMMENT 'Source scenario code'
         ,SRC_SCE_ELM_DSC             VARCHAR(500)                    COMMENT 'Source scenario description'
         ,CBU_COD                     VARCHAR(10)                     COMMENT 'CBU/Market'
         ,SRC_SCE_TYP_COD             VARCHAR(30)                     COMMENT 'Type of scenario: Actual, Rolling Forecast, GPS, Flex or Flat file'
         ,YEA_COD                     NUMBER(10,0)                    COMMENT 'Scenario year'
         ,FLX_STS_COD                 VARCHAR(50)                     COMMENT 'Status of the scenario copy from CC to Flex: cc_only > flex_dependency > copy_done | copy_failed'
         ,T_REC_DLT_FLG               NUMBER(2,0)                     COMMENT '[Technical] Physical deletion flag'
         ,T_REC_SRC_TST               TIMESTAMP_TZ                    COMMENT '[Technical] Source Timestamp'
         ,T_REC_INS_TST               TIMESTAMP_TZ                    COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST               TIMESTAMP_TZ                    COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_SRC_SCE_FLX PRIMARY KEY (SRC_SCE_ELM_COD)
         ) COMMENT = '[Flex] Source scenario masterdata (from Controlling Cloud or Flat Files) - Only the scenarios that are necessary for Flex';
