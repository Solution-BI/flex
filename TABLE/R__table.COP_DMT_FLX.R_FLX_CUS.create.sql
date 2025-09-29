USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_CUS 
         (CUS_ELM_KEY         VARCHAR(30)                             COMMENT 'Customer Key : concatenation of CBU and Customer Code'
         ,CUS_ELM_COD         VARCHAR(30)                             COMMENT 'Customer code'
         ,CUS_ELM_DSC         VARCHAR(500)                            COMMENT 'Customer name'
         ,CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market'
         ,LV1_CUS_COD         VARCHAR(30)                             COMMENT 'L1 Customer code (C hierarchy)'
         ,LV1_CUS_DSC         VARCHAR(500)                            COMMENT 'L1 Customer name (C hierarchy)'
         ,LV2_CUS_COD         VARCHAR(30)                             COMMENT 'L2 Customer code (C hierarchy)'
         ,LV2_CUS_DSC         VARCHAR(500)                            COMMENT 'L2 Customer name (C hierarchy)'
         ,LV3_CUS_COD         VARCHAR(30)                             COMMENT 'L3 Customer code (C hierarchy)'
         ,LV3_CUS_DSC         VARCHAR(500)                            COMMENT 'L3 Customer name (C hierarchy)'
         ,LV4_CUS_COD         VARCHAR(30)                             COMMENT 'L4 Customer code (C hierarchy)'
         ,LV4_CUS_DSC         VARCHAR(500)                            COMMENT 'L4 Customer name (C hierarchy)'
         ,LV5_CUS_COD         VARCHAR(30)                             COMMENT 'L5 Customer code (C hierarchy)'
         ,LV5_CUS_DSC         VARCHAR(500)                            COMMENT 'L5 Customer name (C hierarchy)'
         ,LV6_CUS_COD         VARCHAR(30)                             COMMENT 'L6 Customer code (C hierarchy)'
         ,LV6_CUS_DSC         VARCHAR(500)                            COMMENT 'L6 Customer name (C hierarchy)'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_CUS PRIMARY KEY (CUS_ELM_KEY)
         ) COMMENT = '[Flex] Customer masterdata';
