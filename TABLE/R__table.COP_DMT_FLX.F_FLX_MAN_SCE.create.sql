USE SCHEMA COP_DMT_FLX;

CREATE SEQUENCE IF NOT EXISTS SEQ__F_FLX_MAN_SCE__ID;

CREATE OR REPLACE TABLE F_FLX_MAN_SCE
         (ID                                  NUMBER             DEFAULT SEQ__F_FLX_MAN_SCE__ID.NEXTVAL COMMENT 'ID for writeback'
         ,MAN_SCE_ELM_KEY                     VARCHAR(64)                                               COMMENT 'Scenario Key : concatenation of CBU and Scenario Code'
         ,MAN_SCE_ELM_COD                     VARCHAR(30)
         ,CBU_COD                             VARCHAR(30)
         ,IND_ELM_USR_DSC                     VARCHAR(50)                                               COMMENT 'informational field only, for display'
         ,IND_ELM_COD                         VARCHAR(30)        DEFAULT '#ERR'                         COMMENT 'Indicator code retrieved from the same logic as what is implemented in CC : ACC/DST/FA'
         ,PER_ELM_COD                         VARCHAR(2)
         ,ETI_ELM_COD                         VARCHAR(30)
         ,CUS_ELM_COD                         VARCHAR(30)
         ,PDT_ELM_COD                         VARCHAR(30)
         ,CAT_TYP_ELM_COD                     VARCHAR(30)        DEFAULT 'NA'
         ,EIB_ELM_COD                         VARCHAR(30)        DEFAULT 'NA'
         ,TTY_ELM_COD                         VARCHAR(30)        DEFAULT 'NA'
         ,SAL_SUP_ELM_COD                     VARCHAR(30)        DEFAULT 'NA'
         ,AMOUNT                              NUMBER(38,15)
         ,ACC_ELM_COD                         VARCHAR(50)
         ,DST_ELM_COD                         VARCHAR(50)
         ,FCT_ARE_ELM_COD                     VARCHAR(50)
         ,MAN_ITM_ERR_DTA                     VARCHAR
         ,PER_ERR_FLG                         NUMBER(2,0)        DEFAULT 0
         ,ETI_ERR_FLG                         NUMBER(2,0)        DEFAULT 0
         ,CUS_ERR_FLG                         NUMBER(2,0)        DEFAULT 0
         ,PDT_ERR_FLG                         NUMBER(2,0)        DEFAULT 0
         ,ACC_ERR_FLG                         NUMBER(2,0)        DEFAULT 0
         ,DST_ERR_FLG                         NUMBER(2,0)        DEFAULT 0
         ,FCT_ARE_ERR_FLG                     NUMBER(2,0)        DEFAULT 0
         ,CAT_TYP_ERR_FLG                     NUMBER(2,0)        DEFAULT 0
         ,EIB_ERR_FLG                         NUMBER(2,0)        DEFAULT 0
         ,TTY_ERR_FLG                         NUMBER(2,0)        DEFAULT 0
         ,MAN_ITM_ERR_FLG                     NUMBER(2,0)        DEFAULT 0
         ,T_REC_DLT_FLG                       NUMBER(2,0)        DEFAULT 0                                       COMMENT '[Technical] Physical deletion flag'
         ,T_REC_INS_TST                       TIMESTAMP_TZ       DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)     COMMENT '[Technical] Timestamp of first insertion into the table'
         ,T_REC_UPD_TST                       TIMESTAMP_TZ       DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)     COMMENT '[Technical] Timestamp of last update into the table'
         ) COMMENT = '[Flex] Manual dataset input table';
