USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE R_FLX_IND
(
    IND_ELM_COD      VARCHAR(30)     NOT NULL              COMMENT 'Indicator code',
    IND_ELM_DSC      VARCHAR         NOT NULL              COMMENT 'Indicator name',
    IND_ELM_DSP_DSC  VARCHAR(60)     NOT NULL              COMMENT 'Display name in PBI',
    IND_ELM_TEK_COD  VARCHAR(6)      NOT NULL              COMMENT 'Technical code, on 6 characters',
    IND_ORD_NUM      NUMBER(38,0)    NOT NULL DEFAULT 999  COMMENT 'Display order',
    PAR_IND_ELM_COD  VARCHAR(30)              DEFAULT ''   COMMENT 'Parent Indicator (i.e. the parent indicator value is the sum of its children)',
    IND_FML_TXT      VARCHAR                  DEFAULT ''   COMMENT 'Indicator formula (used to identify dependencies between indicators)',
    FCA_IND_ELM_COD  VARCHAR(30)              DEFAULT ''   COMMENT 'Indicator to which the indicator has sensitivity regarding the FCA (usually [Volume sold])',
    VAR_IND_ELM_COD  VARCHAR(30)              DEFAULT ''   COMMENT 'Indicator to which the indicator has sensitivity regarding the Variability',
    
    T_REC_ARC_FLG NUMBER(38,0)       NOT NULL DEFAULT 0                  COMMENT '[Technical] Record is archived',
    T_REC_DLT_FLG NUMBER(38,0)       NOT NULL DEFAULT 0                  COMMENT '[Technical] Record is deleted',
    T_REC_SRC_TST TIMESTAMP_NTZ(9)   NOT NULL DEFAULT current_timestamp  COMMENT '[Technical] Last modification date/time in the source system',
    T_REC_INS_TST TIMESTAMP_NTZ(9)   NOT NULL DEFAULT current_timestamp  COMMENT '[Technical] First insertion date/time',
    T_REC_UPD_TST TIMESTAMP_NTZ(9)   NOT NULL DEFAULT current_timestamp  COMMENT '[Technical] Last modification date/time',
    
    CONSTRAINT PK_R_FLX_IND PRIMARY KEY (IND_ELM_COD)
)
COMMENT='Indicators/KPI - Manual input'
;