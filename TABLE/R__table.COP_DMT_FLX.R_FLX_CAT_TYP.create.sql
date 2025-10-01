USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE R_FLX_CAT_TYP
         (CAT_TYP_ELM_KEY     VARCHAR(64)                             COMMENT 'Category Type Key : concatenation of CBU and Category Type Code'
         ,CBU_COD             VARCHAR(10)                             COMMENT 'CBU/Market'
         ,CAT_TYP_ELM_COD     VARCHAR(30)                             COMMENT 'Category Type code'
         ,CAT_TYP_ELM_DSC     VARCHAR(500)                            COMMENT 'Category Type name'
         ,T_REC_DLT_FLG       NUMBER(2,0)                             COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST       TIMESTAMP_TZ                            COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_CAT_TYP PRIMARY KEY (CAT_TYP_ELM_KEY)
         ) COMMENT = 'Category Type masterdata';

DELETE FROM R_FLX_CAT_TYP;

INSERT INTO R_FLX_CAT_TYP
       (CAT_TYP_ELM_KEY
       ,CBU_COD
       ,CAT_TYP_ELM_COD
       ,CAT_TYP_ELM_DSC
       ,T_REC_DLT_FLG
       ,T_REC_INS_TST
       ,T_REC_UPD_TST
       )
VALUES ('DCH-MGR' , 'DCH', 'MGR' , 'Managerial'                ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
--      ,('DCH-L500', 'DCH', 'L500', 'Intercompany Margin (L500)',  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ,('DCH-L500', 'DCH', 'L500', 'Internal Transfers'        ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ,('DCH-NA'  , 'DCH', 'NA'  , 'N/A'                       ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ,('IBE-MGR' , 'IBE', 'MGR' , 'Managerial'                ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
--      ,('IBE-L500', 'IBE', 'L500', 'Intercompany Margin (L500)',  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ,('IBE-L500', 'IBE', 'L500', 'Internal Transfers'        ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ,('IBE-NA'  , 'IBE', 'NA'  , 'N/A'                       ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
     ;
