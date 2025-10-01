USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE F_FLX_TGT_SCE
         (CBU_COD                         VARCHAR(10)
         ,SCENARIO_TYPE_CODE              VARCHAR(10)
         ,SCENARIO_ELEMENT_CODE           VARCHAR(15)
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
         ,indicator_code                  VARCHAR(30)
         ,RATE_CY                         NUMBER(28,15)
         ,RATE_FY                         NUMBER(28,15)
         ,AMOUNT_LC                       NUMBER(27,15)
         ,AMOUNT_EUR_CY                   NUMBER(38,15)
         ,AMOUNT_EUR_FY                   NUMBER(38,15)
         ,AMOUNT_EUR                      NUMBER(38,15)
         ,CUR_COD                         VARCHAR(5)
         ,T_REC_SRC_TST                   TIMESTAMP_NTZ(9)
         ,T_REC_INS_TST                   TIMESTAMP_NTZ(9)
         ,T_REC_UPD_TST                   TIMESTAMP_NTZ(9)
         ) COMMENT = '[Flex] Controling cloud Target scenario to send desaggregation data to CC';

