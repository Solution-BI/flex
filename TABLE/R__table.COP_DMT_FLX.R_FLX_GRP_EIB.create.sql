USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE R_FLX_GRP_EIB 
         (CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market '
         ,EIB_ELM_COD         VARCHAR(30)                             COMMENT 'Business type (EIB) code'
         ,EIB_DIM_GRP_COD     NUMBER(10,0)                            COMMENT 'Agg level ID'
         ,EIB_GRP_COD         VARCHAR(30)                             COMMENT 'Agg level element code'
         ,EIB_GRP_DSC         VARCHAR(500)                            COMMENT 'Agg level element name'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_EIB_GRP PRIMARY KEY (CBU_COD, EIB_ELM_COD, EIB_DIM_GRP_COD)
         ) COMMENT = '[Flex] Business type (EIB) aggregation levels';
