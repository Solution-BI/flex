USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Writeback_Disaggregate()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in F_FLX_SCE_CCD
Author      : Noël Coquio (Solution BI France)
Created On  : 19-09-2024
=========================================================================
Modified On:      Description:                               Author:
04-07-2025        Added new clause in TMP_FLX_SRC_SCE_CCD    Manan SHUDDHO (SBI)
=========================================================================
*/

DECLARE
v_STEP_TABLE         VARCHAR(256);
v_STEP_TABLE_ERR     VARCHAR(256);
V_RUN_ID             VARCHAR(256);
v_STEP_NUM           NUMBER(5,0)  := 0;
V_STEP_BEG_DT        VARCHAR(50);
V_STEP_END_DT        VARCHAR(50);
v_ERR_MSG            VARCHAR(1000);
v_IS_SCENARIO        INTEGER;

v_STS_PROC           VARCHAR(5000);
v_ERR_STEP           NUMBER(2,0);
BEGIN

    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Writeback_Deaggregation','#','DLT',CURRENT_USER);

    v_STEP_TABLE := 'Update all scenarios in_progress:1 to in_progress:2';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    -- 1. Update all scenarios in_progress:1 to in_progress:2

    UPDATE COP_DMT_FLX.R_FLX_SCE
    SET    CCD_STS_COD = 'in_progress:2'
    WHERE  CCD_STS_COD = 'in_progress:1';

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 2. Create the Tempoorary table TMP_FLX_SRC_SCE
   
    v_STEP_TABLE := 'CREATE Tempoorary table TMP_FLX_SRC_SCE for the Source Scenario needed';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    CREATE TEMPORARY TABLE COP_DMT_FLX.TMP_FLX_SRC_SCE_CCD
    AS
    SELECT F_FLX_SRC_SCE.CBU_COD
          ,F_FLX_SRC_SCE.SCE_ELM_COD
          ,F_FLX_SRC_SCE.CFG_ORD_NUM
          ,F_FLX_SRC_SCE.SRC_SCE_ELM_COD
          ,F_FLX_SRC_SCE.IND_GRP_COD
          ,F_FLX_SRC_SCE.IND_GRP_RAT_FLG
          ,F_FLX_SRC_SCE.BGN_PER_ELM_COD
          ,F_FLX_SRC_SCE.END_PER_ELM_COD
          ,F_FLX_SRC_SCE.ETI_GRP_COD
          ,F_FLX_SRC_SCE.CUS_GRP_COD
          ,F_FLX_SRC_SCE.CUS_DIM_GRP_COD
          ,F_FLX_SRC_SCE.PDT_GRP_COD
          ,F_FLX_SRC_SCE.CAT_TYP_GRP_COD
          ,F_FLX_SRC_SCE.EIB_GRP_COD
          ,F_FLX_SRC_SCE.TTY_GRP_COD
          ,F_FLX_SRC_SCE.SAL_SUP_GRP_COD
          ,F_FLX_SRC_SCE.IND_ELM_COD
          ,F_FLX_SRC_SCE.CFG_MOD_FLG
           ,(CASE WHEN F_FLX_SRC_SCE.IND_ELM_COD LIKE 'VOL%' THEN 'VOL'
                  ELSE F_FLX_SRC_SCE.IND_ELM_COD
             END)                                                        FLX_IND_ELM_COD
          ,F_FLX_SRC_SCE.IND_NUM_FLG
          ,F_FLX_SRC_SCE.IND_DEN_FLG
          ,F_FLX_SRC_SCE.PER_ELM_COD
          ,F_FLX_SRC_SCE.ACT_PER_FLG
          ,F_FLX_SRC_SCE.ETI_ELM_COD
          ,F_FLX_SRC_SCE.CUS_ELM_COD
          ,R_FLX_GRP_CUS.CUS_GRP_COD                                     CUS_GRP_ELM_COD
          ,F_FLX_SRC_SCE.PDT_ELM_COD
          ,F_FLX_SRC_SCE.CAT_TYP_ELM_COD
          ,F_FLX_SRC_SCE.EIB_ELM_COD
          ,F_FLX_SRC_SCE.TTY_ELM_COD
          ,F_FLX_SRC_SCE.SAL_SUP_ELM_COD
          ,F_FLX_SRC_SCE.SRC_CUR_COD
          ,F_FLX_SRC_SCE.SCE_CUR_COD
          ,F_FLX_SRC_SCE.CNV_CUR_RAT
          ,F_FLX_SRC_SCE.RTO_DEN_AMT
          ,F_FLX_SRC_SCE.BAS_DEN_AMT
          ,F_FLX_SRC_SCE.AMOUNT * F_FLX_SRC_SCE.CFG_MOD_FLG             AMOUNT
          ,F_FLX_SRC_SCE.ACCOUNT_ELEMENT_CODE
          ,F_FLX_SRC_SCE.DESTINATION_ELEMENT_CODE
          ,F_FLX_SRC_SCE.FUNCTIONAL_AREA_ELEMENT_CODE
          ,F_FLX_SRC_SCE.CATEGORY_ELEMENT_CODE
          ,F_FLX_SRC_SCE.CHANNEL_ELEMENT_CODE
          ,F_FLX_SRC_SCE.IOM_CODE
          ,F_FLX_SRC_SCE.PLANT_CODE
          ,F_FLX_SRC_SCE.ORIGINAL_ACCOUNT_ELEMENT_CODE
          ,F_FLX_SRC_SCE.T_REC_SRC_TST
          ,F_FLX_SRC_SCE.T_REC_INS_TST
          ,F_FLX_SRC_SCE.T_REC_UPD_TST
    FROM   COP_DMT_FLX.F_FLX_SRC_SCE
           INNER JOIN COP_DMT_FLX.R_FLX_SCE ON
           (
              F_FLX_SRC_SCE.CBU_COD     = R_FLX_SCE.CBU_COD     AND
              F_FLX_SRC_SCE.SCE_ELM_COD = R_FLX_SCE.SCE_ELM_COD
           )
           INNER JOIN COP_DMT_FLX.R_FLX_GRP_CUS ON 
           (
              F_FLX_SRC_SCE.CBU_COD      = R_FLX_GRP_CUS.CBU_COD         AND
              F_FLX_SRC_SCE.CUS_ELM_COD  = R_FLX_GRP_CUS.CUS_ELM_COD     AND 
              R_FLX_SCE.CUS_DIM_GRP_COD  = R_FLX_GRP_CUS.CUS_DIM_GRP_COD
           )
    WHERE  R_FLX_SCE.CCD_STS_COD = 'in_progress:2'
    ;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 3. Truncate the table W_FLX_SCE_CCD
    v_STEP_TABLE := 'TRUNCATE W_FLX_SCE_CCD';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SCE_CCD;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 4. Load the data in the table W_FLX_SCE_CCD

    v_STEP_TABLE := 'INSERT INTO W_FLX_SCE_CCD';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    INSERT INTO COP_DMT_FLX.W_FLX_SCE_CCD
               (CBU_COD
               ,SCE_ELM_COD
               ,PER_ELM_COD
               ,ETI_ELM_COD
               ,ACC_ELM_COD
               ,DST_ELM_COD
               ,CUS_ELM_COD
               ,PDT_ELM_COD
               ,IOM_COD
               ,CHL_ELM_COD
               ,BUS_TYP_COD
               ,CAT_ELM_COD
               ,EXT_TYP_COD
               ,TTY_COD
               ,PLT_COD
               ,FCT_ARE_ELM_COD
               ,RVN_AMT_ACT_VAL
               ,CUR_COD
               ,SCE_IND_ELM_COD
               ,SRC_IND_ELM_COD
               ,FLX_IND_ELM_COD
               ,T_REC_ARC_FLG
               ,T_REC_DLT_FLG
               ,T_REC_SRC_TST
               ,T_REC_INS_TST
               ,T_REC_UPD_TST
               )
    WITH W_FLX_SCE_SIM AS (
        SELECT   CBU_COD
                ,SCE_ELM_COD
                ,SCE_ELM_KEY
                ,PER_ELM_COD
                ,PER_ACT_FLG
                ,ETI_ELM_KEY
                ,CUS_ELM_KEY
                ,PDT_ELM_KEY
                ,EIB_ELM_KEY
                ,TTY_ELM_KEY
                ,CAT_TYP_ELM_KEY
                ,SAL_SUP_ELM_KEY
                ,CUR_COD
                ,CNV_RAT_VAL
                ,VL1000_B  + VL1000_I     AS VOL
                ,TL2030_B  + TL2030_I     AS NS
                ,CG3001_B  + CG3001_I     AS MAT_COS
                ,CG3002_B  + CG3002_I     AS MAT_OTH
                ,CG3011_B  + CG3011_I     AS MANUF_COS
                ,CG3012_B  + CG3012_I     AS MANUF_OTH
                ,CG3021_B  + CG3021_I     AS LOG_FTC_IFO
                ,CG3022_B  + CG3022_I     AS LOG_USL
                ,CG3023_B  + CG3023_I     AS LOG_OTH
                ,AP4001_B  + AP4001_I     AS AP_WRK
                ,AP4002_B  + AP4002_I     AS AP_NON_WRK
                ,AP4003_B  + AP4003_I     AS AP_OTH
                ,SF5000_B  + SF5000_I     AS SF
                ,HO5051_B  + HO5051_I     AS HOO_MKT
                ,HO5052_B  + HO5052_I     AS HOO_OPS
                ,HO5053_B  + HO5053_I     AS HOO_DBS
                ,HO5054_B  + HO5054_I     AS HOO_GLFUNC
                ,RD6000_B  + RD6000_I     AS RND
                ,IE7000_B  + IE7000_I     AS OIE
                ,VL1000_B                 AS SRC_VOL
                ,TL2030_B                 AS SRC_NS
                ,CG3001_B                 AS SRC_MAT_COS
                ,CG3002_B                 AS SRC_MAT_OTH
                ,CG3011_B                 AS SRC_MANUF_COS
                ,CG3012_B                 AS SRC_MANUF_OTH
                ,CG3021_B                 AS SRC_LOG_FTC_IFO
                ,CG3022_B                 AS SRC_LOG_USL
                ,CG3023_B                 AS SRC_LOG_OTH
                ,AP4001_B                 AS SRC_AP_WRK
                ,AP4002_B                 AS SRC_AP_NON_WRK
                ,AP4003_B                 AS SRC_AP_OTH
                ,SF5000_B                 AS SRC_SF
                ,HO5051_B                 AS SRC_HOO_MKT
                ,HO5052_B                 AS SRC_HOO_OPS
                ,HO5053_B                 AS SRC_HOO_DBS
                ,HO5054_B                 AS SRC_HOO_GLFUNC
                ,RD6000_B                 AS SRC_RND
                ,IE7000_B                 AS SRC_OIE
        FROM     COP_DMT_FLX.W_FLX_SCE_SIM
        WHERE    VOL         != 0
        OR       NS          != 0
        OR       MAT_COS     != 0
        OR       MAT_OTH     != 0
        OR       MANUF_COS   != 0
        OR       MANUF_OTH   != 0
        OR       LOG_FTC_IFO != 0
        OR       LOG_USL     != 0
        OR       LOG_OTH     != 0
        OR       AP_WRK      != 0
        OR       AP_NON_WRK  != 0
        OR       AP_OTH      != 0
        OR       SF          != 0
        OR       HOO_MKT     != 0
        OR       HOO_OPS     != 0
        OR       HOO_DBS     != 0
        OR       HOO_GLFUNC  != 0
        OR       RND         != 0
        OR       OIE         != 0
     )
    ,UNPIVOT_DATA AS (
        SELECT   CBU_COD
                ,SCE_ELM_COD
                ,PER_ELM_COD
                ,REGEXP_REPLACE(ETI_ELM_KEY,'^' || CBU_COD || '-')         ETI_ELM_COD
                ,REGEXP_REPLACE(CUS_ELM_KEY,'^' || CBU_COD || '-')         CUS_GRP_ELM_COD
                ,REGEXP_REPLACE(PDT_ELM_KEY,'^' || CBU_COD || '-')         PDT_ELM_COD
                ,REGEXP_REPLACE(EIB_ELM_KEY,'^' || CBU_COD || '-')         EIB_ELM_COD
                ,REGEXP_REPLACE(TTY_ELM_KEY,'^' || CBU_COD || '-')         TTY_ELM_COD
                ,REGEXP_REPLACE(CAT_TYP_ELM_KEY,'^' || CBU_COD || '-')     CAT_TYP_ELM_COD
                ,REGEXP_REPLACE(SAL_SUP_ELM_KEY,'^' || CBU_COD || '-')     SAL_SUP_ELM_COD
                ,CUR_COD
                ,CNV_RAT_VAL
                ,IND_ELM_COD                                               IND_ELM_COD
                ,COALESCE(IND_ELM_VAL,0)                                   NUM_VAL
                ,COALESCE(NULLIFZERO(SRC_IND_ELM_VAL),1)                   DEN_VAL
        FROM     W_FLX_SCE_SIM
                 UNPIVOT(IND_ELM_VAL FOR IND_ELM_COD IN (VOL, NS, MAT_COS, MAT_OTH, MANUF_COS, MANUF_OTH, LOG_FTC_IFO, LOG_USL, LOG_OTH, AP_WRK, AP_NON_WRK, AP_OTH, SF, HOO_MKT, HOO_OPS, HOO_DBS, HOO_GLFUNC, RND, OIE))
                 UNPIVOT(SRC_IND_ELM_VAL FOR SRC_IND_ELM_COD IN (SRC_VOL, SRC_NS, SRC_MAT_COS, SRC_MAT_OTH, SRC_MANUF_COS, SRC_MANUF_OTH, SRC_LOG_FTC_IFO, SRC_LOG_USL, SRC_LOG_OTH, SRC_AP_WRK, SRC_AP_NON_WRK, SRC_AP_OTH, SRC_SF, SRC_HOO_MKT, SRC_HOO_OPS, SRC_HOO_DBS, SRC_HOO_GLFUNC, SRC_RND, SRC_OIE))
        WHERE    SRC_IND_ELM_COD = 'SRC_' || IND_ELM_COD
     )
    SELECT UNPIVOT_DATA.CBU_COD                                                    CBU_COD
          ,SUBSTR(UNPIVOT_DATA.SCE_ELM_COD,1,30)                                   SCE_ELM_COD
          ,UNPIVOT_DATA.PER_ELM_COD                                                PER_ELM_COD
          ,'ETI_' || UNPIVOT_DATA.ETI_ELM_COD                                      ETI_ELM_COD
          ,TMP_FLX_SRC_SCE_CCD.ORIGINAL_ACCOUNT_ELEMENT_CODE                       ACC_ELM_COD
          ,TMP_FLX_SRC_SCE_CCD.DESTINATION_ELEMENT_CODE                            DST_ELM_COD
          ,DECODE(TMP_FLX_SRC_SCE_CCD.CUS_ELM_COD
                 ,'NA',TMP_FLX_SRC_SCE_CCD.CUS_ELM_COD
                 ,'CUS_' || TMP_FLX_SRC_SCE_CCD.CUS_ELM_COD)                       CUS_ELM_COD
          ,DECODE(UNPIVOT_DATA.PDT_ELM_COD
                 ,'NA',UNPIVOT_DATA.PDT_ELM_COD
                 ,'PDT_' || UNPIVOT_DATA.PDT_ELM_COD)                              PDT_ELM_COD
          ,DECODE(TMP_FLX_SRC_SCE_CCD.IOM_CODE
                 ,'NA',TMP_FLX_SRC_SCE_CCD.IOM_CODE
                 ,'IOM_' || TMP_FLX_SRC_SCE_CCD.IOM_CODE)                          IOM_COD
          ,DECODE(TMP_FLX_SRC_SCE_CCD.CHANNEL_ELEMENT_CODE
                 ,'NA',TMP_FLX_SRC_SCE_CCD.CHANNEL_ELEMENT_CODE
                 ,'CHN_' || TMP_FLX_SRC_SCE_CCD.CHANNEL_ELEMENT_CODE)              CHL_ELM_COD
          ,DECODE(UNPIVOT_DATA.EIB_ELM_COD
                 ,'NA',UNPIVOT_DATA.EIB_ELM_COD
                 ,'BUS_TYP_' || UNPIVOT_DATA.EIB_ELM_COD)                          BUS_TYP_COD
          ,'CAT_' || TMP_FLX_SRC_SCE_CCD.CATEGORY_ELEMENT_CODE                     CAT_ELM_COD
          ,'NA'                                                                    EXT_TYP_COD
          ,UNPIVOT_DATA.TTY_ELM_COD                                                TTY_ELM_COD
          ,DECODE(TMP_FLX_SRC_SCE_CCD.PLANT_CODE
                 ,'NA',PLANT_CODE
                 ,'PLT_' || TMP_FLX_SRC_SCE_CCD.PLANT_CODE)                        PLT_COD
          ,TMP_FLX_SRC_SCE_CCD.FUNCTIONAL_AREA_ELEMENT_CODE                        FCT_ARE_ELM_COD
/*
          ,ROUND(((TMP_FLX_SRC_SCE_CCD.AMOUNT *
                   COALESCE(NULLIFZERO(UNPIVOT_DATA.NUM_VAL),1) /
                   COALESCE(NULLIFZERO(UNPIVOT_DATA.DEN_VAL),1) ) *
                  COALESCE(TMP_FLX_SRC_SCE_CCD.CNV_CUR_RAT,1)) /
                 (CASE WHEN TMP_FLX_SRC_SCE_CCD.ACCOUNT_ELEMENT_CODE LIKE ANY ('VLM%', 'IND%') THEN 1
                       ELSE 1000
                  END)
                ,15)                                                               RVN_AMT_ACT_VAL
*/
          ,ROUND(((TMP_FLX_SRC_SCE_CCD.AMOUNT *
                   CAST(COALESCE(NULLIFZERO(UNPIVOT_DATA.NUM_VAL),1.0) / COALESCE(NULLIFZERO(UNPIVOT_DATA.DEN_VAL),1.0) AS NUMBER(38,15)) 
                  ) 
                  *
                  COALESCE(TMP_FLX_SRC_SCE_CCD.CNV_CUR_RAT,1.0)
                 ) 
                 / CAST((CASE WHEN TMP_FLX_SRC_SCE_CCD.ACCOUNT_ELEMENT_CODE LIKE ANY ('VLM%', 'IND%') THEN 1.0
                              ELSE 1000.0
                         END) AS NUMBER(38,15))
                ,15)                                                               RVN_AMT_ACT_VAL
          ,TMP_FLX_SRC_SCE_CCD.SRC_CUR_COD                                         CUR_COD
          ,R_FLX_GRP_IND.IND_ELM_COD                                               SCE_IND_ELM_COD
          ,TMP_FLX_SRC_SCE_CCD.IND_ELM_COD                                         SRC_IND_ELM_COD
          ,UNPIVOT_DATA.IND_ELM_COD                                                FLX_IND_ELM_COD
          ,0                                                                       T_REC_ARC_FLG
          ,0                                                                       T_REC_DLT_FLG
          ,CURRENT_TIMESTAMP                                                       T_REC_SRC_TST
          ,CURRENT_TIMESTAMP                                                       T_REC_INS_TST
          ,CURRENT_TIMESTAMP                                                       T_REC_UPD_TST
    FROM   UNPIVOT_DATA
           /* Retreive Flex Scenario information */
           INNER JOIN COP_DMT_FLX.R_FLX_SCE ON
           (
              UNPIVOT_DATA.CBU_COD     = R_FLX_SCE.CBU_COD     AND
              UNPIVOT_DATA.SCE_ELM_COD = R_FLX_SCE.SCE_ELM_COD
           )
           /* Retreive Flex Indicator Group information */
           INNER JOIN COP_DMT_FLX.R_FLX_GRP_IND ON
           (
              UNPIVOT_DATA.IND_ELM_COD = R_FLX_GRP_IND.IND_GRP_COD
           )
           /* Retreive Flex Indicator for the scenario information */
           INNER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON
           (
              P_FLX_SCE_CFG_IND.CBU_COD     = R_FLX_SCE.CBU_COD     AND
              P_FLX_SCE_CFG_IND.SCE_ELM_COD = R_FLX_SCE.SCE_ELM_COD
           )
           /* Retreive Source Scenario information */
           INNER JOIN TMP_FLX_SRC_SCE_CCD ON
           (
              TMP_FLX_SRC_SCE_CCD.CBU_COD         = P_FLX_SCE_CFG_IND.CBU_COD     AND
              TMP_FLX_SRC_SCE_CCD.SCE_ELM_COD     = P_FLX_SCE_CFG_IND.SCE_ELM_COD AND
              TMP_FLX_SRC_SCE_CCD.SRC_SCE_ELM_COD = P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD AND
              TMP_FLX_SRC_SCE_CCD.PER_ELM_COD     = UNPIVOT_DATA.PER_ELM_COD      AND
              TMP_FLX_SRC_SCE_CCD.ETI_ELM_COD     = UNPIVOT_DATA.ETI_ELM_COD      AND
              TMP_FLX_SRC_SCE_CCD.CUS_GRP_ELM_COD = UNPIVOT_DATA.CUS_GRP_ELM_COD  AND
              TMP_FLX_SRC_SCE_CCD.PDT_ELM_COD     = UNPIVOT_DATA.PDT_ELM_COD      AND
              TMP_FLX_SRC_SCE_CCD.EIB_ELM_COD     = UNPIVOT_DATA.EIB_ELM_COD      AND
              TMP_FLX_SRC_SCE_CCD.TTY_ELM_COD     = UNPIVOT_DATA.TTY_ELM_COD      AND
              TMP_FLX_SRC_SCE_CCD.CAT_TYP_ELM_COD = UNPIVOT_DATA.CAT_TYP_ELM_COD  AND
              TMP_FLX_SRC_SCE_CCD.SAL_SUP_ELM_COD = UNPIVOT_DATA.SAL_SUP_ELM_COD  AND
              TMP_FLX_SRC_SCE_CCD.IND_GRP_COD     = P_FLX_SCE_CFG_IND.IND_ELM_COD AND
              TMP_FLX_SRC_SCE_CCD.FLX_IND_ELM_COD = R_FLX_GRP_IND.IND_ELM_COD
           )
    WHERE  TMP_FLX_SRC_SCE_CCD.AMOUNT != 0
    AND    R_FLX_SCE.CCD_STS_COD       = 'in_progress:2'
    ;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 5. Drop the Tempoorary table TMP_FLX_SRC_SCE

    v_STEP_TABLE := 'DROP TEMPORARY TABLE TMP_FLX_SRC_SCE_CCD';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    DROP TABLE COP_DMT_FLX.TMP_FLX_SRC_SCE_CCD;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- Call the procedure to log the end of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

    RETURN 'Success';

EXCEPTION
    WHEN OTHER THEN

         v_ERR_MSG := REPLACE(SQLCODE || ': ' || SQLERRM,'''','"');
         v_STEP_TABLE_ERR := v_STEP_TABLE;

         -- Assign the end date of the step
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

         -- Call the procedure to log the step in error with the error message
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, -1, :v_ERR_MSG, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         v_STEP_TABLE := 'UPDATE STATUS TO failed IN R_FLX_SCE';
         v_STEP_NUM   := v_STEP_NUM + 1;

         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

         -- xx. Update all scenarios init_in_progress to failed in case of failure
         UPDATE COP_DMT_FLX.R_FLX_SCE
         SET    CCD_STS_COD = 'failed'
         WHERE  CCD_STS_COD IN ('in_progress:1','in_progress:2');

         -- Assign the end date of the step
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

         -- Call the procedure to log the step
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         -- Call the procedure to log the end of the process
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

         v_ERR_MSG := v_ERR_MSG || ' in the step ' || v_STEP_TABLE_ERR;

         RETURN v_ERR_MSG;
         RAISE; -- Raise the same exception that you are handling.

END;
$$
;
