USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE W_FLX_SCE_CPY 
         (CBU_COD                     VARCHAR(10)                             COMMENT 'CBU/Market'
         ,SCE_ELM_COD                 VARCHAR(30)                             COMMENT 'Scenario code (generated automatically)'
         ,YEA_COD                     NUMBER(10,0)                            COMMENT 'Scenario year'
         ,ACT_PER_FLG                 NUMBER(2,0)                             COMMENT '1 if the period is closed'
         ,SRC_SCE_COD                 VARCHAR(30)                             COMMENT 'Source scenario for the record'
         ,PER_GRP_COD                 VARCHAR(30)                             COMMENT 'Period (aggregated to the configured level)'
         ,ETI_GRP_COD                 VARCHAR(30)                             COMMENT 'Entity (aggregated to the configured level)'
         ,CUS_GRP_COD                 VARCHAR(30)                             COMMENT 'Customer (aggregated to the configured level)'
         ,PDT_GRP_COD                 VARCHAR(30)                             COMMENT 'Product (aggregated to the configured level)'
         ,EIB_GRP_COD                 VARCHAR(30)                             COMMENT 'Business type/EIB (aggregated to the configured level)'
         ,CAT_GRP_COD                 VARCHAR(30)                             COMMENT 'Category (aggregated to the configured level)'
         ,TTY_GRP_COD                 VARCHAR(30)                             COMMENT 'Territory (aggregated to the configured level)'
         ,CUR_COD                     VARCHAR(30)                             COMMENT 'Scenario input currency'
         ,SRC_NET_SALES_CUR_VAL       NUMBER(32,12)                           COMMENT 'Net Sales amount from the source scenario'
         ,INC_NET_SALES_CUR_VAL       NUMBER(32,12)                           COMMENT 'Incremental Net Sales amount from the simulation'
         ,SRC_COGS_CUR_VAL            NUMBER(32,12)                           COMMENT 'COGS amount from the source scenario'
         ,INC_COGS_CUR_VAL            NUMBER(32,12) DEFAULT 0                 COMMENT 'Incremental COGS amount from the simulation'
         ,INC_FIXED_COGS_CUR_VAL      NUMBER(32,12) DEFAULT 0                 COMMENT 'Incremental Fixed COGS amount from the simulation'
         ,INC_VARIABLE_COGS_CUR_VAL   NUMBER(32,12) DEFAULT 0                 COMMENT 'Incremental Variable COGS amount from the simulation'
         ,T_REC_DLT_FLG               NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag [BUT000.ODS_DELKZ (for SLT)]'
         ,T_REC_INS_TST               TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST               TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_W_FLX_SCE_CPY PRIMARY KEY (SCE_ELM_COD, PER_GRP_COD, ETI_GRP_COD, CUS_GRP_COD, PDT_GRP_COD, EIB_GRP_COD, CAT_GRP_COD, TTY_GRP_COD, CUR_COD)
         ) COMMENT = '[Flex] Flex scenarios to be copied to Controlling Cloud, from the Power ON backend';
