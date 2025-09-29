USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_CUR_RAT 
         (CUR_KEY             VARCHAR(64)                             COMMENT 'Currency Key : concatenation of Currency code and Year Code'
         ,CUR_COD             VARCHAR(30)                             COMMENT 'Currency code'
         ,YEA_COD             NUMBER(10)                              COMMENT 'Year of application of the rate'
         ,CNV_RAT_VAL         NUMBER(12,6)                            COMMENT 'Conversion rate from EUR to the currency: EUR x rate = CUR'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_CUR_RAT PRIMARY KEY (CUR_KEY)
         ) COMMENT = '[Flex] Conversion rates for the currency';
