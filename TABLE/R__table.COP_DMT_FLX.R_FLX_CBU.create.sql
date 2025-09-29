USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_CBU 
         (CBU_COD                  VARCHAR(10)                             COMMENT 'CBU/Market code'
         ,CBU_DSC                  VARCHAR(500)                            COMMENT 'CBU/Market name'
         ,ACT_SRC_SCE_COD          VARCHAR(30)                             COMMENT 'Actual comparable scenario'
         ,CMP_1ST_SRC_SCE_COD      VARCHAR(30)                             COMMENT 'First comparable scenario'
         ,CMP_2ND_SRC_SCE_COD      VARCHAR(30)                             COMMENT 'Second comparable scenario'
         ,CMP_3RD_SRC_SCE_COD      VARCHAR(30)                             COMMENT 'Third comparable scenario'
         ,CUS_DIM_GRP_COD          NUMBER(10,0) DEFAULT 0                  COMMENT 'Customer aggregation level'
         ,EIB_USE_FLG              NUMBER(10,0) DEFAULT 0                  COMMENT 'Flag indicating if the Business type/EIB is used (if 0, will always be set to ''NA'')'
         ,TTY_USE_FLG              NUMBER(10,0) DEFAULT 0                  COMMENT 'Flag indicating if the Territory is used (if 0, will always be set to ''NA'')'
         ,VAR_NS                   NUMBER(6,5)  DEFAULT 1                  COMMENT 'Variability for the Net Sales (sensitivity to Volume sold)'
         ,VAR_MAT_COS              NUMBER(6,5)  DEFAULT 1                  COMMENT 'Variability for the Material Cost of Sales (sensitivity to Volume sold)'
         ,VAR_MAT_OTH              NUMBER(6,5)  DEFAULT 1                  COMMENT 'Variability for the Rest of Material Costs (sensitivity to Volume sold)'
         ,VAR_MANUF_COS            NUMBER(6,5)  DEFAULT 1                  COMMENT 'Variability for the Manuf. Cost of Sales (sensitivity to Volume sold)'
         ,VAR_MANUF_OTH            NUMBER(6,5)  DEFAULT 1                  COMMENT 'Variability for the Rest of Manuf. Costs (sensitivity to Volume sold)'
         ,VAR_LOG_FTC_IFO          NUMBER(6,5)  DEFAULT 1                  COMMENT 'Variability for the Log. FTC IFO (sensitivity to Volume sold)'
         ,VAR_LOG_USL              NUMBER(6,5)  DEFAULT 1                  COMMENT 'Variability for the Log. Log USL (sensitivity to Volume sold)'
         ,VAR_LOG_OTH              NUMBER(6,5)  DEFAULT 1                  COMMENT 'Variability for the Rest of Log. Costs (sensitivity to Volume sold)'
         ,T_REC_DLT_FLG            NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST            TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST            TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_CBU PRIMARY KEY (CBU_COD)
         ) COMMENT = '[Flex] CBU/Market masterdata';

INSERT INTO R_FLX_CBU(CBU_COD,CBU_DSC,ACT_SRC_SCE_COD,CMP_1ST_SRC_SCE_COD,CMP_2ND_SRC_SCE_COD,CMP_3RD_SRC_SCE_COD,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
VALUES ('DCH','DACH','2024_RF03_N0','2023_LFL','2023_LFL','2023_RF12_N0',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
      ,('IBE','Iberia','2024_RF03_N0','2023_LFL','2024_RF01_N1','2024_GPS04_N0',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
      ,('POL','Poland','2024_RF03_N0','2023_LFL','2024_RF01_N1','2024_GPS04_N0',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
      ;
