USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE VIEW V_ETL_FLX_SRC_CMP_SCE
AS
SELECT   CBU_COD
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
        ,T_REC_SRC_TST
        ,T_REC_INS_TST
        ,T_REC_UPD_TST
        ,'SRC_SCE'                              SCE_SRC_TAB_COD
FROM     COP_DMT_FLX.F_FLX_SRC_SCE
UNION ALL
SELECT   CBU_COD
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
        ,T_REC_SRC_TST
        ,T_REC_INS_TST
        ,T_REC_UPD_TST
        ,'CMP_SCE'                         SCE_SRC_TAB_COD
FROM     COP_DMT_FLX.W_FLX_CMP_SCE_FLT;
