USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TRANSIENT TABLE T_FLX_SRC_SCE AS
SELECT   F_FLX_SRC_SCE.CBU_COD
        ,F_FLX_SRC_SCE.SCE_ELM_COD
        ,F_FLX_SRC_SCE.CFG_ORD_NUM
        ,F_FLX_SRC_SCE.SRC_SCE_ELM_COD
        ,F_FLX_SRC_SCE.IND_GRP_COD
        ,F_FLX_SRC_SCE.IND_GRP_RAT_FLG
        ,F_FLX_SRC_SCE.BGN_PER_ELM_COD
        ,F_FLX_SRC_SCE.END_PER_ELM_COD
        ,F_FLX_SRC_SCE.ETI_GRP_COD
        ,F_FLX_SRC_SCE.CUS_GRP_COD
        ,F_FLX_SRC_SCE.CUS_DIM_GRP_COD
        ,F_FLX_SRC_SCE.PDT_GRP_COD
        ,F_FLX_SRC_SCE.CAT_TYP_GRP_COD
        ,F_FLX_SRC_SCE.EIB_GRP_COD
        ,F_FLX_SRC_SCE.TTY_GRP_COD
        ,F_FLX_SRC_SCE.SAL_SUP_GRP_COD
        ,F_FLX_SRC_SCE.IND_ELM_COD
        ,F_FLX_SRC_SCE.IND_NUM_FLG
        ,F_FLX_SRC_SCE.IND_DEN_FLG
        ,F_FLX_SRC_SCE.PER_ELM_COD
        ,F_FLX_SRC_SCE.ACT_PER_FLG
        ,F_FLX_SRC_SCE.ETI_ELM_COD
        ,F_FLX_SRC_SCE.CUS_ELM_COD
        ,F_FLX_SRC_SCE.PDT_ELM_COD
        ,F_FLX_SRC_SCE.CAT_TYP_ELM_COD
        ,F_FLX_SRC_SCE.EIB_ELM_COD
        ,F_FLX_SRC_SCE.TTY_ELM_COD
        ,F_FLX_SRC_SCE.SAL_SUP_ELM_COD
        ,F_FLX_SRC_SCE.SRC_CUR_COD
        ,F_FLX_SRC_SCE.SCE_CUR_COD
        ,F_FLX_SRC_SCE.CNV_CUR_RAT
        ,F_FLX_SRC_SCE.RTO_DEN_AMT
        ,F_FLX_SRC_SCE.BAS_DEN_AMT
        ,F_FLX_SRC_SCE.AMOUNT
        ,F_FLX_SRC_SCE.ACCOUNT_ELEMENT_CODE
        ,F_FLX_SRC_SCE.DESTINATION_ELEMENT_CODE
        ,F_FLX_SRC_SCE.FUNCTIONAL_AREA_ELEMENT_CODE
        ,F_FLX_SRC_SCE.CATEGORY_ELEMENT_CODE
        ,F_FLX_SRC_SCE.CHANNEL_ELEMENT_CODE
        ,F_FLX_SRC_SCE.IOM_CODE
        ,F_FLX_SRC_SCE.PLANT_CODE
        ,F_FLX_SRC_SCE.ORIGINAL_ACCOUNT_ELEMENT_CODE
        ,F_FLX_SRC_SCE.T_REC_SRC_TST
        ,F_FLX_SRC_SCE.T_REC_INS_TST
        ,F_FLX_SRC_SCE.T_REC_UPD_TST
FROM     F_FLX_SRC_SCE
;



CREATE OR REPLACE TABLE F_FLX_SRC_SCE
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
         ) COMMENT = '[Flex] Controling cloud Source scenario used for the aggegation / desaggregation process';

INSERT INTO F_FLX_SRC_SCE
      (CBU_COD
      ,SCE_ELM_COD
      ,CFG_ORD_NUM
      ,SRC_SCE_ELM_COD
      ,IND_GRP_COD
      ,IND_GRP_RAT_FLG
      ,BGN_PER_ELM_COD
      ,END_PER_ELM_COD
      ,ETI_GRP_COD
      ,CUS_GRP_COD
      ,CUS_DIM_GRP_COD
      ,PDT_GRP_COD
      ,CAT_TYP_GRP_COD
      ,EIB_GRP_COD
      ,TTY_GRP_COD
      ,SAL_SUP_GRP_COD
      ,IND_ELM_COD
      ,IND_NUM_FLG
      ,IND_DEN_FLG
      ,PER_ELM_COD
      ,ACT_PER_FLG
      ,ETI_ELM_COD
      ,CUS_ELM_COD
      ,PDT_ELM_COD
      ,CAT_TYP_ELM_COD
      ,EIB_ELM_COD
      ,TTY_ELM_COD
      ,SAL_SUP_ELM_COD
      ,SRC_CUR_COD
      ,SCE_CUR_COD
      ,CNV_CUR_RAT
      ,RTO_DEN_AMT
      ,BAS_DEN_AMT
      ,AMOUNT
      ,ACCOUNT_ELEMENT_CODE
      ,DESTINATION_ELEMENT_CODE
      ,FUNCTIONAL_AREA_ELEMENT_CODE
      ,CATEGORY_ELEMENT_CODE
      ,CHANNEL_ELEMENT_CODE
      ,IOM_CODE
      ,PLANT_CODE
      ,ORIGINAL_ACCOUNT_ELEMENT_CODE
      ,T_REC_SRC_TST
      ,T_REC_INS_TST
      ,T_REC_UPD_TST
      )
SELECT CBU_COD
      ,SCE_ELM_COD
      ,CFG_ORD_NUM
      ,SRC_SCE_ELM_COD
      ,IND_GRP_COD
      ,IND_GRP_RAT_FLG
      ,BGN_PER_ELM_COD
      ,END_PER_ELM_COD
      ,ETI_GRP_COD
      ,CUS_GRP_COD
      ,CUS_DIM_GRP_COD
      ,PDT_GRP_COD
      ,CAT_TYP_GRP_COD
      ,EIB_GRP_COD
      ,TTY_GRP_COD
      ,SAL_SUP_GRP_COD
      ,IND_ELM_COD
      ,IND_NUM_FLG
      ,IND_DEN_FLG
      ,PER_ELM_COD
      ,ACT_PER_FLG
      ,ETI_ELM_COD
      ,CUS_ELM_COD
      ,PDT_ELM_COD
      ,CAT_TYP_ELM_COD
      ,EIB_ELM_COD
      ,TTY_ELM_COD
      ,SAL_SUP_ELM_COD
      ,SRC_CUR_COD
      ,SCE_CUR_COD
      ,CNV_CUR_RAT
      ,RTO_DEN_AMT
      ,BAS_DEN_AMT
      ,AMOUNT
      ,ACCOUNT_ELEMENT_CODE
      ,DESTINATION_ELEMENT_CODE
      ,FUNCTIONAL_AREA_ELEMENT_CODE
      ,CATEGORY_ELEMENT_CODE
      ,CHANNEL_ELEMENT_CODE
      ,IOM_CODE
      ,PLANT_CODE
      ,ORIGINAL_ACCOUNT_ELEMENT_CODE
      ,T_REC_SRC_TST
      ,T_REC_INS_TST
      ,T_REC_UPD_TST
FROM   T_FLX_SRC_SCE;

DROP TABLE T_FLX_SRC_SCE;
