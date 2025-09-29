USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TRANSIENT TABLE W_FLX_SRC_SCE__ACT
         (CBU_COD                             VARCHAR(30)
         ,SCE_ELM_COD                         VARCHAR(30)
         ,CFG_ORD_NUM                         NUMBER(2,0)
         ,SRC_SCE_ELM_COD                     VARCHAR(30)
         ,IND_GRP_COD                         VARCHAR(30)
         ,IND_GRP_RAT_FLG                     BOOLEAN
         ,BGN_PER_ELM_COD                     VARCHAR(30)
         ,END_PER_ELM_COD                     VARCHAR(30)
         ,ETI_GRP_COD                         VARCHAR(30)
         ,CUS_GRP_COD                         VARCHAR(30)
         ,CUS_DIM_GRP_COD                     VARCHAR(30)
         ,PDT_GRP_COD                         VARCHAR(30)
         ,CAT_TYP_GRP_COD                     VARCHAR(30)
         ,EIB_GRP_COD                         VARCHAR(30)
         ,TTY_GRP_COD                         VARCHAR(30)
         ,SAL_SUP_GRP_COD                     VARCHAR(30)
         ,IND_ELM_COD                         VARCHAR(30)                                              COMMENT 'Indicator code'
         ,CFG_MOD_FLG                         SMALLINT          DEFAULT 1                              COMMENT 'Allows to manage a config as negative (if -1)'
         ,IND_NUM_FLG                         BOOLEAN
         ,IND_DEN_FLG                         BOOLEAN
         ,PER_ELM_COD                         VARCHAR(2)
         ,ACT_PER_FLG                         BOOLEAN
         ,ETI_ELM_COD                         VARCHAR(30)
         ,CUS_ELM_COD                         VARCHAR(30)
         ,PDT_ELM_COD                         VARCHAR(30)
         ,CAT_TYP_ELM_COD                     VARCHAR(30)
         ,EIB_ELM_COD                         VARCHAR(30)
         ,TTY_ELM_COD                         VARCHAR(30)
         ,SAL_SUP_ELM_COD                     VARCHAR(30)
         ,SRC_CUR_COD                         VARCHAR(5)
         ,SCE_CUR_COD                         VARCHAR(30)
         ,CNV_CUR_RAT                         NUMBER(28,15)
         ,RTO_DEN_AMT                         NUMBER(38,15)
         ,BAS_DEN_AMT                         NUMBER(38,15)
         ,AMOUNT                              NUMBER(38,15)
         ,ACCOUNT_ELEMENT_CODE                VARCHAR(30)
         ,DESTINATION_ELEMENT_CODE            VARCHAR(30)
         ,FUNCTIONAL_AREA_ELEMENT_CODE        VARCHAR(30)
         ,CATEGORY_ELEMENT_CODE               VARCHAR(30)
         ,CHANNEL_ELEMENT_CODE                VARCHAR(30)
         ,IOM_CODE                            VARCHAR(30)
         ,PLANT_CODE                          VARCHAR(30)
         ,ORIGINAL_ACCOUNT_ELEMENT_CODE       VARCHAR(30)
         ,T_REC_SRC_TST                       TIMESTAMP_NTZ(9)
         ,T_REC_INS_TST                       TIMESTAMP_LTZ(9)
         ,T_REC_UPD_TST                       TIMESTAMP_LTZ(9)
         ) DATA_RETENTION_TIME_IN_DAYS = 0 COMMENT = '[Flex] Source scenario Working table to replace data with the actual closed periods';

