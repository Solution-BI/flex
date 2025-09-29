USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TRANSIENT TABLE W_FLX_SRC_SCE
(         CBU_COD                         VARCHAR(10)
         ,SCENARIO_TYPE_CODE              VARCHAR(10)
         ,SCENARIO_ELEMENT_CODE           VARCHAR(30)
         ,RVN_DATE                        DATE
         ,SCENARIO_DATE                   DATE
         ,PERIOD_ELEMENT_CODE             VARCHAR(2)
         ,ENTITY_ELEMENT_CODE             VARCHAR(30)
         ,ACCOUNT_ELEMENT_CODE            VARCHAR(30)
         ,DESTINATION_ELEMENT_CODE        VARCHAR(30)
         ,FUNCTIONAL_AREA_ELEMENT_CODE    VARCHAR(30)
         ,SU_SP_SPLIT_CODE                VARCHAR(30)
         ,CUSTOMER_ELEMENT_CODE           VARCHAR(30)
         ,PRODUCT_ELEMENT_CODE            VARCHAR(30)
         ,INNO_RENO_KEY_CODE              VARCHAR(30)
         ,PRODUCT_ATTRIBUTE_KEY_CODE      VARCHAR(61)
         ,CATEGORY_ELEMENT_CODE           VARCHAR(30)
         ,MGR_L500_CODE                   VARCHAR(30)
         ,EIB_ELEMENT_CODE                VARCHAR(30)
         ,TERRITORY_ELEMENT_CODE          VARCHAR(30)
         ,CHANNEL_ELEMENT_CODE            VARCHAR(30)
         ,IOM_CODE                        VARCHAR(30)
         ,PLANT_CODE                      VARCHAR(30)
         ,ORIGINAL_ACCOUNT_ELEMENT_CODE   VARCHAR(30)
         ,indicator_code                  VARCHAR(30)                                              COMMENT 'Indicator code'
         ,CFG_MOD_FLG                     SMALLINT          DEFAULT 1                              COMMENT 'Allows to manage a config as negative (if -1)'
         ,RATE_CY                         NUMBER(28,15)
         ,RATE_FY                         NUMBER(28,15)
         ,AMOUNT_LC                       NUMBER(38,15)
         ,AMOUNT_EUR_CY                   NUMBER(38,15)
         ,AMOUNT_EUR_FY                   NUMBER(38,15)
         ,AMOUNT_EUR                      NUMBER(38,15)
         ,CUR_COD                         VARCHAR(5)
         ,T_REC_SRC_TST                   TIMESTAMP_NTZ(9)
         ,T_REC_INS_TST                   TIMESTAMP_NTZ(9)
         ,T_REC_UPD_TST                   TIMESTAMP_NTZ(9)
         ) DATA_RETENTION_TIME_IN_DAYS = 0 COMMENT = '[Flex] Working table Controling cloud Source scenario used for the aggegation process';

