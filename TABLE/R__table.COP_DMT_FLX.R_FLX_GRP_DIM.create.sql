USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE R_FLX_GRP_DIM 
         (FLX_DIM_COD        VARCHAR(50)                           COMMENT 'Flex dimension code'
         ,FLX_GRP_COD        NUMBER(10,0)                          COMMENT 'Id for the agg level'
         ,FLX_GRP_DSC        VARCHAR(500)                          COMMENT 'User-friendly name for the agg level'
         ,T_REC_DLT_FLG      NUMBER(2,0)                           COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST      TIMESTAMP_TZ                          COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST      TIMESTAMP_TZ                          COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_DIM PRIMARY KEY (FLX_DIM_COD)
         ) COMMENT = '[Flex] Aggregation levels in each Flex dimension';


INSERT INTO R_FLX_GRP_DIM (FLX_DIM_COD, FLX_GRP_COD, FLX_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
VALUES    --Period
          -- ('PER', -1, 'Total Period',  0, current_timestamp, current_timestamp)
          ('PER',  0, 'Period (LL)' ,  0, current_timestamp, current_timestamp)
         ,('PER',  1, 'Quarter'     ,  0, current_timestamp, current_timestamp)
         ,('PER',  2, 'Semester'    ,  0, current_timestamp, current_timestamp)
         ,('PER',  3, 'Year'        ,  0, current_timestamp, current_timestamp)

          -- Entity
         ,('ETI', -1, 'Total Entity',  0, current_timestamp, current_timestamp)
         ,('ETI',  0, 'Entity'      ,  0, current_timestamp, current_timestamp)
         ,('ETI',  1, 'Country'     ,  0, current_timestamp, current_timestamp)
--         ,('ETI',  2, 'Market'      ,  0, current_timestamp, current_timestamp)

          -- Customer  -- New dimensions will be added to manage disaggregation
         ,('CUS', -1, 'Total Customer',  0, current_timestamp, current_timestamp)
         ,('CUS',  0, 'Customer'      ,  0, current_timestamp, current_timestamp)
         ,('CUS',  1, 'L1 Customer'   ,  0, current_timestamp, current_timestamp)
         ,('CUS',  2, 'L2 Customer'   ,  0, current_timestamp, current_timestamp)
         ,('CUS',  3, 'L3 Customer'   ,  0, current_timestamp, current_timestamp)
         ,('CUS',  4, 'L4 Customer'   ,  0, current_timestamp, current_timestamp)
         ,('CUS',  5, 'L5 Customer'   ,  0, current_timestamp, current_timestamp)

          -- Product  -- New dimensions will be added to manage disaggregation
          --   Process to document if agg on one of the hierarchies: all others will be set to "TOTAL"
         ,('PDT',   -1, 'Total Product'         ,  0, current_timestamp, current_timestamp)
         ,('PDT',    0, 'Product'               ,  0, current_timestamp, current_timestamp)
         ,('PDT',    1, 'L1L Product Category'  ,  0, current_timestamp, current_timestamp)
         ,('PDT',    2, 'L2L Umbrella Brand'    ,  0, current_timestamp, current_timestamp)
         ,('PDT',    3, 'L3L Brand'             ,  0, current_timestamp, current_timestamp)
         ,('PDT',    4, 'L4L Product Family'    ,  0, current_timestamp, current_timestamp)
         ,('PDT',    5, 'L5L Product Sub-Family',  0, current_timestamp, current_timestamp)
         ,('PDT',    6, 'L6L Product Nature'    ,  0, current_timestamp, current_timestamp)
         ,('PDT',   10, 'L0 Product Category'   ,  0, current_timestamp, current_timestamp)
         ,('PDT',   11, 'L1 Product Area'       ,  0, current_timestamp, current_timestamp)
         ,('PDT',   12, 'L2 Umbrella Brand'     ,  0, current_timestamp, current_timestamp)
         ,('PDT',   13, 'L3 Product Brand'      ,  0, current_timestamp, current_timestamp)
         ,('PDT',   14, 'L4 Product Family'     ,  0, current_timestamp, current_timestamp)
         ,('PDT',   15, 'L5 Product Sub-Family' ,  0, current_timestamp, current_timestamp)
         ,('PDT',   16, 'L6 Product Nature'     ,  0, current_timestamp, current_timestamp)
         ,('PDT',  101, 'L1 Platform Brand'     ,  0, current_timestamp, current_timestamp)
         ,('PDT',  102, 'L2 Brand'              ,  0, current_timestamp, current_timestamp)
         ,('PDT',  103, 'L3 Sub-Brand'          ,  0, current_timestamp, current_timestamp)
          
          -- EIB (Business Type)
         ,('EIB', -1, 'Total EIB',  0, current_timestamp, current_timestamp)
         ,('EIB',  0, 'EIB'      ,  0, current_timestamp, current_timestamp)
          
          -- Managerial/L500
         ,('CAT_TYP', -1, 'Total Managerial/L500',  0, current_timestamp, current_timestamp)
         ,('CAT_TYP',  0, 'Managerial/L500 (LL)' ,  0, current_timestamp, current_timestamp)
          
          -- Territory
         ,('TTY', -1, 'Total Territory',  0, current_timestamp, current_timestamp)
         ,('TTY',  0, 'Territory'      ,  0, current_timestamp, current_timestamp)
          
          -- SU/SP Split
         ,('SAL_SUP', -1, 'Total SU/SP'     ,  0, current_timestamp, current_timestamp)
         ,('SAL_SUP',  0, 'SU/SP Split (LL)',  0, current_timestamp, current_timestamp)
          -- Category
         ,('CAT', -1, 'Total Category',  0, current_timestamp, current_timestamp)
         ,('CAT',  0, 'Category'      ,  0, current_timestamp, current_timestamp)
;

