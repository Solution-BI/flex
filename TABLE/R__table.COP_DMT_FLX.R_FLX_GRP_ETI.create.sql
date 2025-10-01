USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE R_FLX_GRP_ETI 
         (CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market'
         ,ETI_ELM_COD         VARCHAR(30)                             COMMENT 'Entity code'
         ,ETI_DIM_GRP_COD     NUMBER(10,0)                            COMMENT 'Agg level ID'
         ,ETI_GRP_COD         VARCHAR(30)                             COMMENT 'Agg level element code'
         ,ETI_GRP_DSC         VARCHAR(500)                            COMMENT 'Agg level element name'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_GRP_ETI PRIMARY KEY (CBU_COD, ETI_ELM_COD, ETI_DIM_GRP_COD)
         ) COMMENT = '[Flex] Entity aggregation levels';
