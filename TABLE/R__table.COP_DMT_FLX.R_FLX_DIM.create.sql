USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_DIM 
         (FLX_DIM_COD      VARCHAR(50)                             COMMENT 'Flex dimension code'
         ,FLX_DIM_DSC      VARCHAR(500)                            COMMENT 'Flex dimension name'
         ,T_REC_DLT_FLG    NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST    TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST    TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_DIM PRIMARY KEY (FLX_DIM_COD)
         ) COMMENT = '[Flex] Flex simulation dimensions';


INSERT INTO R_FLX_DIM (FLX_DIM_COD, FLX_DIM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
VALUES ('PER'    , 'Period'                          ,  0, current_timestamp, current_timestamp)
      ,('ETI'    , 'Entity'                          ,  0, current_timestamp, current_timestamp)
      ,('CUS'    , 'Customer'                        ,  0, current_timestamp, current_timestamp)
      ,('PDT'    , 'Product'                         ,  0, current_timestamp, current_timestamp)
      ,('EIB'    , 'EIB (Business Type)'             ,  0, current_timestamp, current_timestamp)
      ,('CAT_TYP', 'Managerial / Internal Transfers' ,  0, current_timestamp, current_timestamp)
      ,('TTY'    , 'Territory'                       ,  0, current_timestamp, current_timestamp)
      ,('SAL_SUP', 'SU/SP Split'                     ,  0, current_timestamp, current_timestamp)
      ;