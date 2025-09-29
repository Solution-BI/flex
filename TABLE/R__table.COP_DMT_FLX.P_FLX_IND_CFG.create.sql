USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE P_FLX_IND_CFG
         (IND_ELM_COD                            VARCHAR(30)                   COMMENT 'Indicator code'
         ,CFG_MOD_FLG                            SMALLINT                      COMMENT 'Allows to manage a config as negative (if -1)'
         ,ACC01_L5_GL_ACCOUNT_CODE               VARCHAR                       COMMENT 'Filter on Account L5 Category Code: L5 G/L Account (single-value for inclusions or comma-separated values for !exclusions)'
         ,ACC01_L3_SUB_CATEGORY_CODE             VARCHAR                       COMMENT 'Filter on Account L3 Category Code: L3 Sub-category (single-value for inclusions or comma-separated values for !exclusions)'
         ,ACC01_L1_MACRO_CATEGORY_CODE           VARCHAR                       COMMENT 'Filter on Account L1 Category Code: L1 Macro Category (single-value for inclusions or comma-separated values for !exclusions)'
         ,ACC01_ACCOUNT_TYPE_CODE                VARCHAR                       COMMENT 'Filter on Account type code (single-value for inclusions or comma-separated values for !exclusions)'
         ,ACC02_ACC_TOP_NOD                      VARCHAR                       COMMENT 'Filter on Account WAP Top node (single-value for inclusions or comma-separated values for !exclusions)'
         ,ACC03_ACC_TOP_NOD                      VARCHAR                       COMMENT 'Filter on Account NWAP Top node (single-value for inclusions or comma-separated values for !exclusions)'
         ,L1_CUSTOMER_DISTRIBUTION_CHANNEL_DESC  VARCHAR                       COMMENT 'Filter on L1 Customer desc: Distribution Channel description (single-value for inclusions or comma-separated values for !exclusions)'
         ,DST01_L3_DESTINATION_CODE              VARCHAR                       COMMENT 'Filter on CC L3 Destination Code (single-value for inclusions or comma-separated values for !exclusions)'
         ,DST01_L2_DESTINATION_CODE              VARCHAR                       COMMENT 'Filter on CC L2 Destination Code (single-value for inclusions or comma-separated values for !exclusions)'
         ,DST01_L1_DESTINATION_CODE              VARCHAR                       COMMENT 'Filter on CC L1 Destination Code (single-value for inclusions or comma-separated values for !exclusions)'
         ,FCT_ARE_ELM_COD                        VARCHAR                       COMMENT 'Filter on Functional Area Element Code (single-value for inclusions or comma-separated values for !exclusions)'
         ,FCT_ARE_LVL1_NOD                       VARCHAR                       COMMENT 'Filter on Functional Area L1 Node Code (single-value for inclusions or comma-separated values for !exclusions)'
         ,IOM_TYP_COD                            VARCHAR                       COMMENT 'Filter on IOM Type (single-value for inclusions or comma-separated values for !exclusions)'
         ,T_REC_ARC_FLG                          SMALLINT                      COMMENT '[Technical] Record is archived'
         ,T_REC_DLT_FLG                          SMALLINT                      COMMENT '[Technical] Record is deleted'
         ,T_REC_SRC_TST                          TIMESTAMP_NTZ                 COMMENT '[Technical] Last modification date/time in the source system'
         ,T_REC_INS_TST                          TIMESTAMP_NTZ                 COMMENT '[Technical] First insertion date/time'
         ,T_REC_UPD_TST                          TIMESTAMP_NTZ                 COMMENT '[Technical] Last modification date/time'
         ) COMMENT = '[Flex] Indicators/KPI configuration (field starting with "!" denotes an exclusion list (comma-separated) | inclusion is single-value) - Manual maintenance';
