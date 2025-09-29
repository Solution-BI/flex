USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TRANSIENT TABLE W_FLX_SCE_SIM__IN_INI  
         (CBU_COD                            VARCHAR(10)                        COMMENT 'CBU/Market'
         ,SCE_ELM_COD                        VARCHAR(30)                        COMMENT 'Scenario code'
         ,SCE_ELM_KEY                        VARCHAR(64)                        COMMENT 'Scenario Key'
         ,PER_ELM_COD                        VARCHAR(30) 
         ,PER_ACT_FLG                        NUMBER(2,0)
         ,ETI_ELM_KEY                        VARCHAR(64)
         ,CUS_ELM_KEY                        VARCHAR(64)
         ,PDT_ELM_KEY                        VARCHAR(64)
         ,EIB_ELM_KEY                        VARCHAR(64)
         ,TTY_ELM_KEY                        VARCHAR(64)
         ,SAL_SUP_ELM_KEY                    VARCHAR(64)
         ,CAT_TYP_ELM_KEY                    VARCHAR(64)
          -- Volume
         ,VL1000_B                           NUMBER(32,12)
         ,VL1000_I                           NUMBER(32,12) DEFAULT 0
          -- Net Sales
         ,TL2030_B                           NUMBER(32,12)
         ,TL2030_I                           NUMBER(32,12) DEFAULT 0
         ,V_TL2030                           NUMBER(32,12)
          -- Material Costs
         ,CG3001_B                           NUMBER(32,12)
         ,CG3001_I                           NUMBER(32,12) DEFAULT 0
         ,V_CG3001                           NUMBER(32,12)
          -- Material Other Costs
         ,CG3002_B                           NUMBER(32,12)
         ,CG3002_I                           NUMBER(32,12) DEFAULT 0
         ,A_CG3002                           NUMBER(32,12)
         ,V_CG3002                           NUMBER(32,12)
          -- Manufacturing Costs
         ,CG3011_B                           NUMBER(32,12)
         ,CG3011_I                           NUMBER(32,12) DEFAULT 0
         ,V_CG3011                           NUMBER(32,12)
          -- Manufacturing Other Costs
         ,CG3012_B                           NUMBER(32,12)
         ,CG3012_I                           NUMBER(32,12) DEFAULT 0
         ,A_CG3012                           NUMBER(32,12)
         ,V_CG3012                           NUMBER(32,12)
          -- Logistic FTC_IFO Costs
         ,CG3021_B                           NUMBER(32,12)
         ,CG3021_I                           NUMBER(32,12) DEFAULT 0
         ,V_CG3021                           NUMBER(32,12)
          -- Logistic USL Costs
         ,CG3022_B                           NUMBER(32,12)
         ,CG3022_I                           NUMBER(32,12) DEFAULT 0
         ,V_CG3022                           NUMBER(32,12)
          -- Logistic Other Costs
         ,CG3023_B                           NUMBER(32,12)
         ,CG3023_I                           NUMBER(32,12) DEFAULT 0
         ,A_CG3023                           NUMBER(32,12)
         ,V_CG3023                           NUMBER(32,12)
          -- AP_WRK
         ,AP4001_B                           NUMBER(32,12)
         ,AP4001_I                           NUMBER(32,12) DEFAULT 0
          -- AP_NON_WRK
         ,AP4002_B                           NUMBER(32,12)
         ,AP4002_I                           NUMBER(32,12) DEFAULT 0
          -- AP_OTH
         ,AP4003_B                           NUMBER(32,12)
         ,AP4003_I                           NUMBER(32,12) DEFAULT 0
          -- Sales Force Costs
         ,SF5000_B                           NUMBER(32,12)
         ,SF5000_I                           NUMBER(32,12) DEFAULT 0
          -- HOO_MKT
         ,HO5051_B                           NUMBER(32,12)
         ,HO5051_I                           NUMBER(32,12) DEFAULT 0
          -- HOO_OPS
         ,HO5052_B                           NUMBER(32,12)
         ,HO5052_I                           NUMBER(32,12) DEFAULT 0
          -- HOO_DBS
         ,HO5053_B                           NUMBER(32,12)
         ,HO5053_I                           NUMBER(32,12) DEFAULT 0
          -- HOO_GLFUNC
         ,HO5054_B                           NUMBER(32,12)
         ,HO5054_I                           NUMBER(32,12) DEFAULT 0
          -- RND
         ,RD6000_B                           NUMBER(32,12)
         ,RD6000_I                           NUMBER(32,12) DEFAULT 0
          -- OIE
         ,IE7000_B                           NUMBER(32,12)
         ,IE7000_I                           NUMBER(32,12) DEFAULT 0
         ,T_REC_INS_TST                      TIMESTAMP_TZ
         ,T_REC_UPD_TST                      TIMESTAMP_TZ
         ) DATA_RETENTION_TIME_IN_DAYS = 0 COMMENT = '[Flex] Simulation table used to copy the data from SQLServer for the initialisation process';


