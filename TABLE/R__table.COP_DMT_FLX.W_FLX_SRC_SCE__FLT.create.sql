USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TRANSIENT TABLE W_FLX_SRC_SCE__FLT
         (SCE_ELM_KEY                             VARCHAR(64)                                              COMMENT 'Flex Scenario key'
         ,CBU_COD                                 VARCHAR(10)                                              COMMENT 'CBU/Market code'
         ,SCE_ELM_COD                             VARCHAR(50)                                              COMMENT 'Flex Scenario code'
         ,CFG_ORD_NUM                             NUMBER(2,0)                                              COMMENT 'Order of priority (order of insertion by default)'
         ,SRC_SCE_ELM_COD                         VARCHAR(30)                                              COMMENT 'Source Scenario for the configured combination'
         ,IND_GRP_COD                             VARCHAR(30)                                              COMMENT 'Indicator code'
         ,IND_GRP_RAT_FLG                         BOOLEAN                                                  COMMENT 'Indicator Group Ratio Flag (1 : Yes / 0 : No)'
         ,BGN_PER_ELM_COD                         VARCHAR(30)                                              COMMENT 'Filter on the Period - Beginning'
         ,END_PER_ELM_COD                         VARCHAR(30)                                              COMMENT 'Filter on the Period - End'
         ,ETI_GRP_COD                             VARCHAR(30)                                              COMMENT 'Filter in the Entity dimension'
         ,CUS_GRP_COD                             VARCHAR(30)                                              COMMENT 'Filter in the Customer dimension'
         ,CUS_DIM_GRP_COD                         NUMBER(10,0)                                             COMMENT 'Customer Dimension Code (use for disagregation)'
         ,PDT_GRP_COD                             VARCHAR(30)                                              COMMENT 'Filter in the Product dimension'
         ,CAT_TYP_GRP_COD                         VARCHAR(30)                                              COMMENT 'Managerial/Interco Margin filter'
         ,EIB_GRP_COD                             VARCHAR(30)                                              COMMENT 'EIB filter'
         ,TTY_GRP_COD                             VARCHAR(30)                                              COMMENT 'Filter in the Territory dimension'
         ,SAL_SUP_GRP_COD                         VARCHAR(30)                                              COMMENT 'SU/SP filter'
         ,IND_ELM_COD                             VARCHAR(30)                                              COMMENT 'Indicator code'
         ,CFG_MOD_FLG                             SMALLINT          DEFAULT 1                              COMMENT 'Allows to manage a config as negative (if -1)'
         ,IND_NUM_FLG                             BOOLEAN                                                  COMMENT 'Indicator Numerator Flag (1 : Yes / 0 : No)'
         ,IND_DEN_FLG                             BOOLEAN                                                  COMMENT 'Indicator Denominator Flag (1 : Yes / 0 : No)'
         ,PER_ELM_COD                             VARCHAR(2)
         ,ACT_PER_FLG                             NUMBER(2,0)
         ,ETI_ELM_COD                             VARCHAR(30)
         ,CUS_ELM_COD                             VARCHAR(30)
         ,PDT_ELM_COD                             VARCHAR(30)
         ,CAT_TYP_ELM_COD                         VARCHAR(30)
         ,EIB_ELM_COD                             VARCHAR(30)
         ,TTY_ELM_COD                             VARCHAR(30)
         ,SAL_SUP_ELM_COD                         VARCHAR(30)
         ,SRC_CUR_COD                             VARCHAR(5)
         ,SCE_CUR_COD                             VARCHAR(30)                                              COMMENT 'Scenario input currency'
         ,CNV_CUR_RAT                             NUMBER(28,15)
         ,AMOUNT                                  NUMBER(38,15)
         ,ACCOUNT_ELEMENT_CODE                    VARCHAR(30)
         ,DESTINATION_ELEMENT_CODE                VARCHAR(30)
         ,FUNCTIONAL_AREA_ELEMENT_CODE            VARCHAR(30)
         ,CATEGORY_ELEMENT_CODE                   VARCHAR(30)
         ,CHANNEL_ELEMENT_CODE                    VARCHAR(30)
         ,IOM_CODE                                VARCHAR(30)
         ,PLANT_CODE                              VARCHAR(30)
         ,ORIGINAL_ACCOUNT_ELEMENT_CODE           VARCHAR(30)
         ,T_REC_SRC_TST                           TIMESTAMP_NTZ(9)
         ,T_REC_INS_TST                           TIMESTAMP_NTZ(9)
         ,T_REC_UPD_TST                           TIMESTAMP_NTZ(9)
         ) DATA_RETENTION_TIME_IN_DAYS = 0 COMMENT = '[Flex] Working table Controling cloud Source scenario used for the aggegation process';
;
