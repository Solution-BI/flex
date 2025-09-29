USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR ALTER TABLE F_FLX_SRC_SCE_IND 
         (CBU_COD                 VARCHAR(10)                     COMMENT 'CBU/Market'
         ,SRC_SCE_ELM_COD         VARCHAR(30)                     COMMENT 'Scenario code (generated automatically)'
         ,PER_GRP_COD             VARCHAR(30)                     COMMENT 'Period (aggregated to the configured level)'
         ,ETI_GRP_COD             VARCHAR(30)                     COMMENT 'Entity (aggregated to the configured level)'
         ,CUS_GRP_COD             VARCHAR(30)                     COMMENT 'Customer (aggregated to the configured level)'
         ,PDT_GRP_COD             VARCHAR(30)                     COMMENT 'Product (aggregated to the configured level)'
         ,EIB_GRP_COD             VARCHAR(30)                     COMMENT 'Business type/EIB (aggregated to the configured level)'
         ,CAT_GRP_COD             VARCHAR(30)                     COMMENT 'Category (aggregated to the configured level)'
         ,TTY_GRP_COD             VARCHAR(30)                     COMMENT 'Territory (aggregated to the configured level)'
         ,CUR_COD                 NUMBER(10,0)                    COMMENT 'Scenario input currency'
         ,CNV_RAT_VAL             NUMBER(12,6)                    COMMENT 'Conversion factor from local to scenario currency'
         ,IND_ELM_COD             VARCHAR(30)                     COMMENT 'Indicator code'
         ,FLX_SRC_CUR_VAL         NUMBER(32,12)                   COMMENT 'Indicator amount, in scenario currency'
         ,T_REC_DLT_FLG           NUMBER(2,0)                     COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST           TIMESTAMP_TZ                    COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST           TIMESTAMP_TZ                    COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_F_FLX_SRC_SCE_IND PRIMARY KEY (CBU_COD, SRC_SCE_ELM_COD, PER_GRP_COD, ETI_GRP_COD, CUS_GRP_COD, PDT_GRP_COD, EIB_GRP_COD, CAT_GRP_COD, TTY_GRP_COD, CUR_COD, IND_ELM_COD)
         ) COMMENT = '[Flex] Flex source scenarios, aggregated to the right level';
