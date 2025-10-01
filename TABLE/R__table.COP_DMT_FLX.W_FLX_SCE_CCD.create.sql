USE SCHEMA COP_DMT_FLX;

DROP TABLE IF EXISTS F_FLX_SCE_CCD;

CREATE OR REPLACE TRANSIENT TABLE W_FLX_SCE_CCD 
         (CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market'
         ,SCE_ELM_COD         VARCHAR(30)                             COMMENT 'Scenario code (generated automatically)'
         ,PER_ELM_COD         VARCHAR(30)                             COMMENT 'Period Code'
         ,ETI_ELM_COD         VARCHAR(30)                             COMMENT 'Entity Code'
         ,ACC_ELM_COD         VARCHAR(30)                             COMMENT 'Account Code'
         ,DST_ELM_COD         VARCHAR(30)                             COMMENT 'Destination Code'
         ,CUS_ELM_COD         VARCHAR(30)                             COMMENT 'Customer Code'
         ,PDT_ELM_COD         VARCHAR(30)                             COMMENT 'Material Code'
         ,IOM_COD             VARCHAR(30)                             COMMENT 'Internal Order'
         ,CHL_ELM_COD         VARCHAR(30)                             COMMENT 'Channel  Code'
         ,BUS_TYP_COD         VARCHAR(30)                             COMMENT 'Business Type'
         ,CAT_ELM_COD         VARCHAR(30)                             COMMENT 'Category Code'
         ,EXT_TYP_COD         VARCHAR(30)
         ,TTY_COD             VARCHAR(30)                             COMMENT 'Territory Code'
         ,PLT_COD             VARCHAR(30)                             COMMENT 'Plant Code'
         ,FCT_ARE_ELM_COD     VARCHAR(30)                             COMMENT 'Functional Area Code'
         ,RVN_AMT_ACT_VAL     NUMBER(38,15)                           COMMENT 'Element Value'
         ,CUR_COD             VARCHAR(5)                              COMMENT 'Local Currency Code'
         ,SCE_IND_ELM_COD     VARCHAR(30)                             COMMENT 'Scenario Indicator Code'
         ,SRC_IND_ELM_COD     VARCHAR(30)                             COMMENT 'Source Indicator Code'
         ,FLX_IND_ELM_COD     VARCHAR(30)                             COMMENT 'Flex Indicator Code'
         ,T_REC_ARC_FLG       NUMBER(38,0)                            COMMENT '[Technical] Archive flag'
         ,T_REC_DLT_FLG       NUMBER(38,0)                            COMMENT '[Technical] Physical deletion flag'
         ,T_REC_SRC_TST       TIMESTAMP_NTZ(9)                        COMMENT '[Technical] Modification timestamp in the source system [R_FLX_SCE.T_REC_UPD_TST]'
         ,T_REC_INS_TST       TIMESTAMP_NTZ(9)                        COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_NTZ(9)                        COMMENT '[Technical] Timestamp of last update into the table'
         ) DATA_RETENTION_TIME_IN_DAYS = 0 COMMENT = '[Flex] Working table used for the deaggegation process';

