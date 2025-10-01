USE SCHEMA COP_DMT_FLX;

/*
CREATE OR REPLACE TABLE F_FLX_SCE_INI_BCK
AS
SELECT    SCE_ELM_KEY
         ,CBU_COD
         ,SCE_ELM_COD
         ,DTA_YEA_COD
         ,PER_ELM_COD
         ,ACT_PER_FLG
         ,ETI_ELM_COD
         ,CUS_ELM_COD
         ,PDT_ELM_COD
         ,CAT_TYP_ELM_COD
         ,EIB_ELM_COD
         ,TTY_ELM_COD
         ,SAL_SUP_ELM_COD
         ,CUR_COD
         ,CNV_RAT_VAL
         ,VOL_UNT_COD
         ,BASE_VOL
         ,INCR_VOL
         ,CMP1_VOL
         ,CMP2_VOL
         ,CMP3_VOL
         ,BASE_GS
         ,INCR_GS
         ,CMP1_GS
         ,CMP2_GS
         ,CMP3_GS
         ,BASE_SD
         ,INCR_SD
         ,CMP1_SD
         ,CMP2_SD
         ,CMP3_SD
         ,BASE_DR
         ,INCR_DR
         ,CMP1_DR
         ,CMP2_DR
         ,CMP3_DR
         ,BASE_TS
         ,INCR_TS
         ,CMP1_TS
         ,CMP2_TS
         ,CMP3_TS
         ,BASE_MAT
         ,INCR_MAT
         ,CMP1_MAT
         ,CMP2_MAT
         ,CMP3_MAT
         ,BASE_MAT_VAR
         ,INCR_MAT_VAR
         ,CMP1_MAT_VAR
         ,CMP2_MAT_VAR
         ,CMP3_MAT_VAR
         ,BASE_MANUF
         ,INCR_MANUF
         ,CMP1_MANUF
         ,CMP2_MANUF
         ,CMP3_MANUF
         ,BASE_MANUF_VAR
         ,INCR_MANUF_VAR
         ,CMP1_MANUF_VAR
         ,CMP2_MANUF_VAR
         ,CMP3_MANUF_VAR
         ,BASE_LOG
         ,INCR_LOG
         ,CMP1_LOG
         ,CMP2_LOG
         ,CMP3_LOG
         ,BASE_LOG_VAR
         ,INCR_LOG_VAR
         ,CMP1_LOG_VAR
         ,CMP2_LOG_VAR
         ,CMP3_LOG_VAR
         ,CONF_COGS_TOT_FCA
         ,INCR_FCA
         ,BASE_AP
         ,INCR_AP
         ,CMP1_AP
         ,CMP2_AP
         ,CMP3_AP
         ,BASE_SFO
         ,INCR_SFO
         ,CMP1_SFO
         ,CMP2_SFO
         ,CMP3_SFO
         ,BASE_HOO
         ,INCR_HOO
         ,CMP1_HOO
         ,CMP2_HOO
         ,CMP3_HOO
         ,BASE_RNI
         ,INCR_RNI
         ,CMP1_RNI
         ,CMP2_RNI
         ,CMP3_RNI
         ,BASE_OIE
         ,INCR_OIE
         ,CMP1_OIE
         ,CMP2_OIE
         ,CMP3_OIE
         ,PCT_NS
         ,PCT_FCOGS
         ,PCT_VCOGS
         ,T_REC_INS_TST
         ,T_REC_UPD_TST
FROM      F_FLX_SCE_INI;

INSERT INTO F_FLX_SCE_INI
SELECT * 
FROM   F_FLX_SCE_INI_BCK;

DROP TABLE F_FLX_SCE_INI_BCK;

*/

CREATE OR REPLACE TABLE F_FLX_SCE_INI
         (SCE_ELM_KEY               VARCHAR(64)
         ,CBU_COD                   VARCHAR(10)
         ,SCE_ELM_COD               VARCHAR(50)
         ,DTA_YEA_COD               NUMBER(10,0)
         ,PER_ELM_COD               VARCHAR(2)
         ,ACT_PER_FLG               NUMBER(1,0)
         ,ETI_ELM_COD               VARCHAR(30)
         ,CUS_ELM_COD               VARCHAR(30)
         ,PDT_ELM_COD               VARCHAR(30)
         ,CAT_TYP_ELM_COD           VARCHAR(30)
         ,EIB_ELM_COD               VARCHAR(30)
         ,TTY_ELM_COD               VARCHAR(30)
         ,SAL_SUP_ELM_COD           VARCHAR(30)
         ,CUR_COD                   VARCHAR(30)
         ,CNV_RAT_VAL               NUMBER(12,6)
         ,VOL_UNT_COD               VARCHAR(7)
         ,BASE_VOL                  NUMBER(38,15)
         ,INCR_VOL                  NUMBER(1,0)
         ,CMP1_VOL                  NUMBER(38,15)
         ,CMP2_VOL                  NUMBER(38,15)
         ,CMP3_VOL                  NUMBER(38,15)
         ,BASE_GS                   NUMBER(38,15)
         ,INCR_GS                   NUMBER(1,0)
         ,CMP1_GS                   NUMBER(38,15)
         ,CMP2_GS                   NUMBER(38,15)
         ,CMP3_GS                   NUMBER(38,15)
         ,BASE_SD                   NUMBER(38,15)
         ,INCR_SD                   NUMBER(1,0)
         ,CMP1_SD                   NUMBER(38,15)
         ,CMP2_SD                   NUMBER(38,15)
         ,CMP3_SD                   NUMBER(38,15)
         ,BASE_DR                   NUMBER(38,15)
         ,INCR_DR                   NUMBER(1,0)
         ,CMP1_DR                   NUMBER(38,15)
         ,CMP2_DR                   NUMBER(38,15)
         ,CMP3_DR                   NUMBER(38,15)
         ,BASE_TS                   NUMBER(38,15)
         ,INCR_TS                   NUMBER(1,0)
         ,CMP1_TS                   NUMBER(38,15)
         ,CMP2_TS                   NUMBER(38,15)
         ,CMP3_TS                   NUMBER(38,15)
         ,BASE_MAT                  NUMBER(38,15)
         ,INCR_MAT                  NUMBER(1,0)
         ,CMP1_MAT                  NUMBER(38,15)
         ,CMP2_MAT                  NUMBER(38,15)
         ,CMP3_MAT                  NUMBER(38,15)
         ,BASE_MAT_FIX              NUMBER(38,15)
         ,INCR_MAT_FIX              NUMBER(1,0)
         ,CMP1_MAT_FIX              NUMBER(38,15)
         ,CMP2_MAT_FIX              NUMBER(38,15)
         ,CMP3_MAT_FIX              NUMBER(38,15)
         ,BASE_MAT_VAR              NUMBER(38,15)
         ,INCR_MAT_VAR              NUMBER(1,0)
         ,CMP1_MAT_VAR              NUMBER(38,15)
         ,CMP2_MAT_VAR              NUMBER(38,15)
         ,CMP3_MAT_VAR              NUMBER(38,15)
         ,BASE_MANUF                NUMBER(38,15)
         ,INCR_MANUF                NUMBER(1,0)
         ,CMP1_MANUF                NUMBER(38,15)
         ,CMP2_MANUF                NUMBER(38,15)
         ,CMP3_MANUF                NUMBER(38,15)
         ,BASE_MANUF_FIX            NUMBER(38,15)
         ,INCR_MANUF_FIX            NUMBER(1,0)
         ,CMP1_MANUF_FIX            NUMBER(38,15)
         ,CMP2_MANUF_FIX            NUMBER(38,15)
         ,CMP3_MANUF_FIX            NUMBER(38,15)
         ,BASE_MANUF_VAR            NUMBER(38,15)
         ,INCR_MANUF_VAR            NUMBER(1,0)
         ,CMP1_MANUF_VAR            NUMBER(38,15)
         ,CMP2_MANUF_VAR            NUMBER(38,15)
         ,CMP3_MANUF_VAR            NUMBER(38,15)
         ,BASE_LOG                  NUMBER(38,15)
         ,INCR_LOG                  NUMBER(1,0)
         ,CMP1_LOG                  NUMBER(38,15)
         ,CMP2_LOG                  NUMBER(38,15)
         ,CMP3_LOG                  NUMBER(38,15)
         ,BASE_LOG_FIX              NUMBER(38,15)
         ,INCR_LOG_FIX              NUMBER(1,0)
         ,CMP1_LOG_FIX              NUMBER(38,15)
         ,CMP2_LOG_FIX              NUMBER(38,15)
         ,CMP3_LOG_FIX              NUMBER(38,15)
         ,BASE_LOG_VAR              NUMBER(38,15)
         ,INCR_LOG_VAR              NUMBER(1,0)
         ,CMP1_LOG_VAR              NUMBER(38,15)
         ,CMP2_LOG_VAR              NUMBER(38,15)
         ,CMP3_LOG_VAR              NUMBER(38,15)
         ,CONF_COGS_TOT_FCA         NUMBER(38,15)
         ,INCR_FCA                  NUMBER(1,0)
         ,BASE_AP                   NUMBER(38,15)
         ,INCR_AP                   NUMBER(1,0)
         ,CMP1_AP                   NUMBER(38,15)
         ,CMP2_AP                   NUMBER(38,15)
         ,CMP3_AP                   NUMBER(38,15)
         ,BASE_SFO                  NUMBER(38,15)
         ,INCR_SFO                  NUMBER(1,0)
         ,CMP1_SFO                  NUMBER(38,15)
         ,CMP2_SFO                  NUMBER(38,15)
         ,CMP3_SFO                  NUMBER(38,15)
         ,BASE_HOO                  NUMBER(38,15)
         ,INCR_HOO                  NUMBER(1,0)
         ,CMP1_HOO                  NUMBER(38,15)
         ,CMP2_HOO                  NUMBER(38,15)
         ,CMP3_HOO                  NUMBER(38,15)
         ,BASE_RNI                  NUMBER(38,15)
         ,INCR_RNI                  NUMBER(1,0)
         ,CMP1_RNI                  NUMBER(38,15)
         ,CMP2_RNI                  NUMBER(38,15)
         ,CMP3_RNI                  NUMBER(38,15)
         ,BASE_OIE                  NUMBER(38,15)
         ,INCR_OIE                  NUMBER(1,0)
         ,CMP1_OIE                  NUMBER(38,15)
         ,CMP2_OIE                  NUMBER(38,15)
         ,CMP3_OIE                  NUMBER(38,15)
         ,PCT_NS                    NUMBER(6,5)
         ,PCT_FCOGS                 NUMBER(6,5)
         ,PCT_VCOGS                 NUMBER(6,5)
         ,T_REC_INS_TST             TIMESTAMP_NTZ(9)
         ,T_REC_UPD_TST             TIMESTAMP_NTZ(9)
         ) COMMENT = '[Flex] Initialization table used to send the data to SQLServer';
