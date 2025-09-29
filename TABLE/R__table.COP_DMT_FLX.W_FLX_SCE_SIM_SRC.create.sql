USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TRANSIENT TABLE W_FLX_SCE_SIM_SRC 
         (CBU_COD                            VARCHAR(10)                        COMMENT 'CBU/Market'
         ,SCE_ELM_COD                        VARCHAR(30)                        COMMENT 'Scenario code'
         ,SCE_ELM_KEY                        VARCHAR(64)                        COMMENT 'Scenario Key'
         ,PER_ELM_COD                        VARCHAR(30) 
         ,PER_ACT_FLG                        NUMBER(2,0)
         ,ETI_ELM_KEY                        VARCHAR(64)
         ,CUS_ELM_KEY                        VARCHAR(64)
         ,LV0_PDT_CAT_COD                    VARCHAR(30)
         ,PDT_ELM_KEY                        VARCHAR(64)
         ,EIB_ELM_KEY                        VARCHAR(64)
         ,TTY_ELM_KEY                        VARCHAR(64)
         ,CAT_TYP_ELM_KEY                    VARCHAR(64)
         ,SAL_SUP_ELM_KEY                    VARCHAR(64)
         ,CUR_COD                            VARCHAR(10)
         ,CNV_RAT_VAL                        NUMBER(12,6)
         ,SRC_VOL                            NUMBER(32,12)
         ,INC_VOL                            NUMBER(32,12)
         ,PCT_VOL                            NUMBER(32,12) DEFAULT 0
         ,SRC_NS                             NUMBER(32,12)
         ,INC_NS                             NUMBER(32,12)
         ,PCT_NS                             NUMBER(32,12)
         ,SRC_COGS                           NUMBER(32,12)
         ,INC_COGS                           NUMBER(32,12)
         ,PCT_COGS                           NUMBER(32,12) DEFAULT 0
         ,SRC_VCOGS                          NUMBER(32,12)
         ,INC_VCOGS                          NUMBER(32,12)
         ,PCT_VCOGS                          NUMBER(32,12)
         ,SRC_FCOGS                          NUMBER(32,12)
         ,INC_FCOGS                          NUMBER(32,12)
         ,PCT_FCOGS                          NUMBER(32,12)
         ,SRC_FCA                            NUMBER(32,12)
         ,INC_FCA                            NUMBER(32,12)
         ,PCT_FCA                            NUMBER(32,12) DEFAULT 0
         ,SRC_AP                             NUMBER(32,12)
         ,INC_AP                             NUMBER(32,12)
         ,PCT_AP                             NUMBER(32,12) DEFAULT 0
         ,SRC_OIE                            NUMBER(32,12)
         ,INC_OIE                            NUMBER(32,12)
         ,PCT_OIE                            NUMBER(32,12) DEFAULT 0
         ,SRC_FC                             NUMBER(32,12)
         ,INC_FC                             NUMBER(32,12)
         ,PCT_FC                             NUMBER(32,12) DEFAULT 0
         ,T_REC_INS_TST                      TIMESTAMP_TZ
         ,T_REC_UPD_TST                      TIMESTAMP_TZ
         ) DATA_RETENTION_TIME_IN_DAYS = 0 COMMENT = '[Flex] Simulation table used to copy the data from SQLServer';


