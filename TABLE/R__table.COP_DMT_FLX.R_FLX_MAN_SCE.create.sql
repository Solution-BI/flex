USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE R_FLX_MAN_SCE 
         (MAN_SCE_ELM_KEY              VARCHAR(64)                          COMMENT 'Dataset Key : concatenation of CBU and Dataset Code'
         ,MAN_SCE_ELM_COD              VARCHAR(50)                          COMMENT 'Dataset  code (generated automatically)'
         ,MAN_SCE_ELM_DSC              VARCHAR(50)                          COMMENT 'Dataset description'
         ,CBU_COD                      VARCHAR(10)                          COMMENT 'CBU/Market'
         ,MAN_SCE_CUR_COD              VARCHAR(30)                          COMMENT 'Dataset input currency'
         ,MAN_SCE_USE_FLG              NUMBER(2,0)   DEFAULT 0              COMMENT 'Dataset configuration completion flag, Once selected, data cannot be modified anymore and the manual data is available to use as Source for Flex scenarios'
         ,CRE_USR_COD                  VARCHAR(50)                          COMMENT 'Power BI user who created the dataset - suppress the domain information ie @.....'
         ,CRE_END_TST                  TIMESTAMP                            COMMENT 'Date and time of creation of the Dataset'
         ,MAN_SCE_EML_USR_COD          VARCHAR(50)                          COMMENT 'Power BI user who created the dataset with the domain'
         ,MAN_SCE_DLT_STS_COD          VARCHAR(30)                          COMMENT 'Deletion status: na > requested > in_progress > done | failed'
         ,MAN_SCE_DLT_FLG              NUMBER(2,0)   DEFAULT 0              COMMENT 'Flag indicating that a user has requested the dataset to be deleted (Power ON writeback)'
         ,MAN_SCE_DLT_RQT_TST          TIMESTAMP                            COMMENT 'Date and time of the deletion request'
         ,MAN_SCE_DLT_RQT_EML_USR_COD  VARCHAR(50)                          COMMENT 'Email of the user who requested the deletion'
         ,MAN_SCE_DLT_END_TST          TIMESTAMP                            COMMENT 'Date and time the deletion process finished (changed to done or failed status)'
         ,MAN_SCE_CMT_TXT              VARCHAR(5000)                        COMMENT 'Comment on the Dataset'
         ,MAN_SCE_CMT_TST              TIMESTAMP                            COMMENT 'Date and Time of last modification of the comment'
         ,MAN_SCE_CMT_EML_USR_COD      VARCHAR(50)                          COMMENT 'Email of the user who modified the comment last'
         ,T_REC_DLT_FLG                NUMBER(2,0)                          COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST                TIMESTAMP_TZ                         COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST                TIMESTAMP_TZ                         COMMENT '[Technical] Timestamp of last update into the table'
         ,CONSTRAINT PK_R_FLX_MAN_SCE PRIMARY KEY (MAN_SCE_ELM_KEY)
         ) COMMENT = '[Flex] Manual Dataset masterdata'
;
