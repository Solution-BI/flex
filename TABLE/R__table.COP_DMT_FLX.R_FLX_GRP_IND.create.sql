USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE R_FLX_GRP_IND
(         IND_GRP_COD                             VARCHAR(120)                            COMMENT 'Indicator Group Code'
         ,IND_ELM_COD                             VARCHAR(120)                            COMMENT 'Indicator Element Code'
         ,IND_GRP_RAT_FLG                         BOOLEAN                                 COMMENT 'Indicator Group Ratio Flag (1 : Yes / 0 : No)'
         ,IND_NUM_FLG                             BOOLEAN                                 COMMENT 'Indicator Numerator Flag (1 : Yes / 0 : No)'
         ,IND_DEN_FLG                             BOOLEAN                                 COMMENT 'Indicator Denominator Flag (1 : Yes / 0 : No)'
         ,T_REC_UPD_TST                           TIMESTAMP_TZ                            COMMENT '[Technical] Last modification date/time'
)COMMENT = '[FLEX] Indicator/KPI Group';