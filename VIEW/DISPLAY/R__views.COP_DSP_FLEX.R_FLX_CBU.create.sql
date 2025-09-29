USE SCHEMA COP_DSP_FLEX{{uid}};

CREATE OR REPLACE VIEW R_FLX_CBU 
         (CBU_COD                         COMMENT 'CBU/Market code'
         ,CBU_DSC                         COMMENT 'CBU/Market name'
         ,ACT_SRC_SCE_COD                 COMMENT 'Actual comparable scenario'
         ,CMP_1ST_SRC_SCE_COD             COMMENT 'First comparable scenario'
         ,CMP_2ND_SRC_SCE_COD             COMMENT 'Second comparable scenario'
         ,CMP_3RD_SRC_SCE_COD             COMMENT 'Third comparable scenario'
         ,CUS_DIM_GRP_COD                 COMMENT 'Customer aggregation level'
         ,EIB_USE_FLG                     COMMENT 'Flag indicating if the Business type/EIB is used (if 0, will always be set to ''NA'')'
         ,TTY_USE_FLG                     COMMENT 'Flag indicating if the Territory is used (if 0, will always be set to ''NA'')'
         ,VAR_NS                          COMMENT 'Variability for the Net Sales (sensitivity to Volume sold)'
         ,VAR_MAT_COS                     COMMENT 'Variability for the Material Cost of Sales (sensitivity to Volume sold)'
         ,VAR_MAT_OTH                     COMMENT 'Variability for the Rest of Material Costs (sensitivity to Volume sold)'
         ,VAR_MANUF_COS                   COMMENT 'Variability for the Manuf. Cost of Sales (sensitivity to Volume sold)'
         ,VAR_MANUF_OTH                   COMMENT 'Variability for the Rest of Manuf. Costs (sensitivity to Volume sold)'
         ,VAR_LOG_FTC_IFO                 COMMENT 'Variability for the Log. FTC IFO (sensitivity to Volume sold)'
         ,VAR_LOG_USL                     COMMENT 'Variability for the Log. Log USL (sensitivity to Volume sold)'
         ,VAR_LOG_OTH                     COMMENT 'Variability for the Rest of Log. Costs (sensitivity to Volume sold)'
         ,T_REC_DLT_FLG                   COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST                   COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST                   COMMENT '[Technical] Timestamp of last update into the table'
         ) COMMENT = '[Flex] CBU/Market masterdata'
AS
SELECT    CBU_COD
         ,CBU_DSC
         ,ACT_SRC_SCE_COD
         ,CMP_1ST_SRC_SCE_COD
         ,CMP_2ND_SRC_SCE_COD
         ,CMP_3RD_SRC_SCE_COD
         ,CUS_DIM_GRP_COD
         ,EIB_USE_FLG
         ,TTY_USE_FLG
         ,VAR_NS
         ,VAR_MAT_COS
         ,VAR_MAT_OTH
         ,VAR_MANUF_COS
         ,VAR_MANUF_OTH
         ,VAR_LOG_FTC_IFO
         ,VAR_LOG_USL
         ,VAR_LOG_OTH
         ,T_REC_DLT_FLG
         ,T_REC_INS_TST
         ,T_REC_UPD_TST
FROM      COP_DMT_FLX.R_FLX_CBU;