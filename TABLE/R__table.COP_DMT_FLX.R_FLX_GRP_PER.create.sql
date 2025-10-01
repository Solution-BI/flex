USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE R_FLX_GRP_PER 
         (CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market'
         ,PER_ELM_COD         VARCHAR(30)                             COMMENT 'Period code'
         ,PER_DIM_GRP_COD     NUMBER(10,0)                            COMMENT 'Agg level ID'
         ,PER_GRP_COD         VARCHAR(30)                             COMMENT 'Agg level element code'
         ,PER_GRP_DSC         VARCHAR(500)                            COMMENT 'Agg level element name'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_GRP_PER PRIMARY KEY (CBU_COD, PER_ELM_COD, PER_DIM_GRP_COD)
         ) COMMENT = '[Flex] Period aggregation levels';
