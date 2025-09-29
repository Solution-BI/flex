USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE PROCEDURE SP_Flex_Ini_Disaggregate()
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
Created On  : 20-11-2024 
=========================================================================
Modified On:      Description:    still in DEV PART           Author:
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);
V_RUN_ID         VARCHAR(256);
v_STEP_NUM       NUMBER(5,0)  := 0;
V_STEP_BEG_DT    VARCHAR(50);
V_STEP_END_DT    VARCHAR(50);
v_ERR_MSG        VARCHAR(1000);
v_IS_SCENARIO    INTEGER;

v_STS_PROC       VARCHAR(5000);
v_ERR_STEP       NUMBER(2,0);
BEGIN

    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Ini_Disaggregate','#','DLT',CURRENT_USER);

    -- 1. Create the Tempoorary table TMP_FLX_SRC_SCE_FLX
    
    v_STEP_TABLE := 'CREATE Tempoorary table TMP_FLX_SRC_SCE_FLX for the Source Scenario needed';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    CREATE TEMPORARY TABLE COP_DMT_FLX.TMP_FLX_SRC_SCE_FLX
    AS
    WITH FLX_SCE AS (
        SELECT  DISTINCT
                R_FLX_SCE_SRC.CBU_COD          CBU_COD
               ,R_FLX_SCE_SRC.SCE_ELM_COD      SCE_ELM_COD
               ,R_FLX_SCE_SRC.CUS_DIM_GRP_COD  CUS_DIM_GRP_COD
        FROM    COP_DMT_FLX.R_FLX_SCE
                INNER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON
                (
                     R_FLX_SCE.SCE_ELM_KEY = P_FLX_SCE_CFG_IND.SCE_ELM_KEY
                )
                INNER JOIN COP_DMT_FLX.R_FLX_SCE R_FLX_SCE_SRC ON
                (
                     R_FLX_SCE_SRC.CBU_COD      = P_FLX_SCE_CFG_IND.CBU_COD         AND
                     R_FLX_SCE_SRC.SCE_ELM_COD  = P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD AND
                     R_FLX_SCE_SRC.INI_STS_COD  = 'done'                            AND
                     R_FLX_SCE_SRC.CCD_STS_COD != 'done'                            AND
                     R_FLX_SCE_SRC.DLT_STS_COD != 'done'
                )
        WHERE   R_FLX_SCE.INI_STS_COD NOT IN ('created','requested','done','failed')
    )
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
          ,R_FLX_GRP_CUS.CUS_GRP_COD                                    CUS_GRP_ELM_COD
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
           INNER JOIN FLX_SCE ON
           (
                F_FLX_SRC_SCE.CBU_COD     = FLX_SCE.CBU_COD     AND
                F_FLX_SRC_SCE.SCE_ELM_COD = FLX_SCE.SCE_ELM_COD
           )
           INNER JOIN COP_DMT_FLX.R_FLX_GRP_CUS ON 
           (
                F_FLX_SRC_SCE.CBU_COD      = R_FLX_GRP_CUS.CBU_COD         AND
                F_FLX_SRC_SCE.CUS_ELM_COD  = R_FLX_GRP_CUS.CUS_ELM_COD     AND 
                FLX_SCE.CUS_DIM_GRP_COD    = R_FLX_GRP_CUS.CUS_DIM_GRP_COD
           )
    ;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 3. Load the data in the table W_FLX_SRC_SCE_FLX

    v_STEP_TABLE := 'INSERT INTO W_FLX_SRC_SCE_FLX';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    INSERT INTO COP_DMT_FLX.W_FLX_SRC_SCE
           (CBU_COD
           ,SCENARIO_TYPE_CODE
           ,SCENARIO_ELEMENT_CODE
           ,RVN_DATE
           ,SCENARIO_DATE
           ,PERIOD_ELEMENT_CODE
           ,ENTITY_ELEMENT_CODE
           ,ACCOUNT_ELEMENT_CODE
           ,DESTINATION_ELEMENT_CODE
           ,FUNCTIONAL_AREA_ELEMENT_CODE
           ,SU_SP_SPLIT_CODE
           ,CUSTOMER_ELEMENT_CODE
           ,PRODUCT_ELEMENT_CODE
           --,INNO_RENO_KEY_CODE
           --,PRODUCT_ATTRIBUTE_KEY_CODE
           ,CATEGORY_ELEMENT_CODE
           ,MGR_L500_CODE
           ,EIB_ELEMENT_CODE
           ,TERRITORY_ELEMENT_CODE
           ,CHANNEL_ELEMENT_CODE
           ,IOM_CODE
           ,PLANT_CODE
           ,ORIGINAL_ACCOUNT_ELEMENT_CODE
           ,indicator_code
           ,CFG_MOD_FLG
           ,RATE_CY
           ,RATE_FY
           ,AMOUNT_LC
           ,AMOUNT_EUR_CY
           ,AMOUNT_EUR_FY
           ,CUR_COD
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
               ,SAL_SUP_ELM_KEY
               ,CAT_TYP_ELM_KEY
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
       FROM     COP_DMT_FLX.W_FLX_SCE_SIM__IN_INI
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
               ,REGEXP_REPLACE(SAL_SUP_ELM_KEY,'^' || CBU_COD || '-')     SAL_SUP_ELM_COD
               ,REGEXP_REPLACE(CAT_TYP_ELM_KEY,'^' || CBU_COD || '-')     CAT_TYP_ELM_COD
               ,IND_ELM_COD                                               IND_ELM_COD
               ,COALESCE(IND_ELM_VAL,0)                                   NUM_VAL
               ,COALESCE(NULLIFZERO(SRC_IND_ELM_VAL),1)                   DEN_VAL
       FROM     W_FLX_SCE_SIM
                UNPIVOT(IND_ELM_VAL FOR IND_ELM_COD IN (VOL, NS, MAT_COS, MAT_OTH, MANUF_COS, MANUF_OTH, LOG_FTC_IFO, LOG_USL, LOG_OTH, AP_WRK, AP_NON_WRK, AP_OTH, SF, HOO_MKT, HOO_OPS, HOO_DBS, HOO_GLFUNC, RND, OIE))
                UNPIVOT(SRC_IND_ELM_VAL FOR SRC_IND_ELM_COD IN (SRC_VOL, SRC_NS, SRC_MAT_COS, SRC_MAT_OTH, SRC_MANUF_COS, SRC_MANUF_OTH, SRC_LOG_FTC_IFO, SRC_LOG_USL, SRC_LOG_OTH, SRC_AP_WRK, SRC_AP_NON_WRK, SRC_AP_OTH, SRC_SF, SRC_HOO_MKT, SRC_HOO_OPS, SRC_HOO_DBS, SRC_HOO_GLFUNC, SRC_RND, SRC_OIE))
       WHERE    SRC_IND_ELM_COD = 'SRC_' || IND_ELM_COD
    )
   ,FLX_SCE AS (
        SELECT  DISTINCT
                R_FLX_SCE_SRC.CBU_COD        CBU_COD
               ,R_FLX_SCE_SRC.SCE_ELM_COD    SCE_ELM_COD
               ,R_FLX_SCE.CUR_COD            CUR_COD
        FROM    COP_DMT_FLX.R_FLX_SCE
                INNER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON
                (
                     R_FLX_SCE.SCE_ELM_KEY = P_FLX_SCE_CFG_IND.SCE_ELM_KEY
                )
                INNER JOIN COP_DMT_FLX.R_FLX_SCE R_FLX_SCE_SRC ON
                (
                     R_FLX_SCE_SRC.CBU_COD      = P_FLX_SCE_CFG_IND.CBU_COD         AND
                     R_FLX_SCE_SRC.SCE_ELM_COD  = P_FLX_SCE_CFG_IND.SRC_SCE_ELM_COD AND
                     R_FLX_SCE_SRC.INI_STS_COD  = 'done'                            AND
                     R_FLX_SCE_SRC.CCD_STS_COD != 'done'                            AND
                     R_FLX_SCE_SRC.DLT_STS_COD != 'done'
                )
        WHERE   R_FLX_SCE.INI_STS_COD NOT IN ('created','requested','done','failed')
    )
    SELECT UNPIVOT_DATA.CBU_COD                                                    CBU_COD
          ,'FLEX'                                                                  SCENARIO_TYPE_CODE
          ,SUBSTR(UNPIVOT_DATA.SCE_ELM_COD,1,30)                                   SCENARIO_ELEMENT_CODE
          ,NULL                                                                    RVN_DATE
          ,NULL                                                                    SCENARIO_DATE
          ,UNPIVOT_DATA.PER_ELM_COD                                                PERIOD_ELEMENT_CODE
          ,UNPIVOT_DATA.ETI_ELM_COD                                                ENTITY_ELEMENT_CODE
          ,TMP_FLX_SRC_SCE.ACCOUNT_ELEMENT_CODE                                    ACCOUNT_ELEMENT_CODE
          ,TMP_FLX_SRC_SCE.DESTINATION_ELEMENT_CODE                                DESTINATION_ELEMENT_CODE
          ,TMP_FLX_SRC_SCE.FUNCTIONAL_AREA_ELEMENT_CODE                            FUNCTIONAL_AREA_ELEMENT_CODE
          ,UNPIVOT_DATA.SAL_SUP_ELM_COD                                            SU_SP_SPLIT_CODE
          ,TMP_FLX_SRC_SCE.CUS_ELM_COD                                             CUSTOMER_ELEMENT_CODE
          ,UNPIVOT_DATA.PDT_ELM_COD                                                PRODUCT_ELEMENT_CODE
          ,TMP_FLX_SRC_SCE.CATEGORY_ELEMENT_CODE                                   CATEGORY_ELEMENT_CODE
          ,UNPIVOT_DATA.CAT_TYP_ELM_COD                                            MGR_L500_CODE
          ,UNPIVOT_DATA.EIB_ELM_COD                                                EIB_ELEMENT_CODE
          ,UNPIVOT_DATA.TTY_ELM_COD                                                TERRITORY_ELEMENT_CODE
          ,TMP_FLX_SRC_SCE.CHANNEL_ELEMENT_CODE                                    CHANNEL_ELEMENT_CODE
          ,TMP_FLX_SRC_SCE.IOM_CODE                                                IOM_CODE
          ,TMP_FLX_SRC_SCE.PLANT_CODE                                              PLANT_CODE
          ,TMP_FLX_SRC_SCE.ORIGINAL_ACCOUNT_ELEMENT_CODE                           ORIGINAL_ACCOUNT_ELEMENT_CODE
          ,TMP_FLX_SRC_SCE.IND_ELM_COD                                             INDICATOR_CODE
          ,TMP_FLX_SRC_SCE.CFG_MOD_FLG                                             CFG_MOD_FLG
          ,CUR.RATE_CY                                                             RATE_CY
          ,CUR.RATE_FY                                                             RATE_FY
          /* Convert the Flex amount in Local Currency */
          ,ROUND(((TMP_FLX_SRC_SCE.AMOUNT *
                   COALESCE(NULLIFZERO(UNPIVOT_DATA.NUM_VAL),1) /
                   COALESCE(NULLIFZERO(UNPIVOT_DATA.DEN_VAL),1) ) *
                  COALESCE(TMP_FLX_SRC_SCE.CNV_CUR_RAT,1)) 
                ,15)                                                               AMOUNT_LC
          /* Convert the amount in Local Currency in Euro_Current_Year */
          ,ROUND(IFF(TMP_FLX_SRC_SCE.ACCOUNT_ELEMENT_CODE LIKE ANY ('VLM%', 'IND%')
                    ,AMOUNT_LC
                    ,(AMOUNT_LC / COALESCE(NULLIFZERO(CUR.RATE_CY),1))
                    ), 15)                                                         AMOUNT_EUR_CY
          /* Convert the amount in Local Currency in Euro_Future_Year */
          ,ROUND(IFF(TMP_FLX_SRC_SCE.ACCOUNT_ELEMENT_CODE LIKE ANY ('VLM%', 'IND%')
                      -- NOT REALLY AN AMOUNT, SO NO CONVERSION
                    ,AMOUNT_LC
                    ,(AMOUNT_LC / COALESCE(NULLIFZERO(CUR.RATE_FY),1))
                    ), 15)                                                         AMOUNT_EUR_FY
          ,TMP_FLX_SRC_SCE.SRC_CUR_COD                                             CUR_COD
          ,CURRENT_TIMESTAMP                                                       T_REC_SRC_TST
          ,CURRENT_TIMESTAMP                                                       T_REC_INS_TST
          ,CURRENT_TIMESTAMP                                                       T_REC_UPD_TST
    FROM   UNPIVOT_DATA
          /* Retreive Flex Scenario information */
          INNER JOIN FLX_SCE ON
          (
               UNPIVOT_DATA.CBU_COD     = FLX_SCE.CBU_COD     AND
               UNPIVOT_DATA.SCE_ELM_COD = FLX_SCE.SCE_ELM_COD
          )
          /* Retreive Flex Indicator Group information */
          INNER JOIN COP_DMT_FLX.R_FLX_GRP_IND ON
          (
               UNPIVOT_DATA.IND_ELM_COD = R_FLX_GRP_IND.IND_GRP_COD
          )
          /* Retreive Flex Indicator for the scenario information */
          INNER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON
          (
               P_FLX_SCE_CFG_IND.CBU_COD     = FLX_SCE.CBU_COD     AND
               P_FLX_SCE_CFG_IND.SCE_ELM_COD = FLX_SCE.SCE_ELM_COD
          )
          /* Retreive Source Scenario information */
          INNER JOIN TMP_FLX_SRC_SCE_FLX TMP_FLX_SRC_SCE ON
          (
               TMP_FLX_SRC_SCE.CBU_COD         = P_FLX_SCE_CFG_IND.CBU_COD     AND
               TMP_FLX_SRC_SCE.SCE_ELM_COD     = P_FLX_SCE_CFG_IND.SCE_ELM_COD AND
               TMP_FLX_SRC_SCE.PER_ELM_COD     = UNPIVOT_DATA.PER_ELM_COD      AND
               TMP_FLX_SRC_SCE.ETI_ELM_COD     = UNPIVOT_DATA.ETI_ELM_COD      AND
               TMP_FLX_SRC_SCE.CUS_GRP_ELM_COD = UNPIVOT_DATA.CUS_GRP_ELM_COD  AND
               TMP_FLX_SRC_SCE.PDT_ELM_COD     = UNPIVOT_DATA.PDT_ELM_COD      AND
               TMP_FLX_SRC_SCE.EIB_ELM_COD     = UNPIVOT_DATA.EIB_ELM_COD      AND
               TMP_FLX_SRC_SCE.TTY_ELM_COD     = UNPIVOT_DATA.TTY_ELM_COD      AND
               TMP_FLX_SRC_SCE.CAT_TYP_ELM_COD = UNPIVOT_DATA.CAT_TYP_ELM_COD  AND
               TMP_FLX_SRC_SCE.SAL_SUP_ELM_COD = UNPIVOT_DATA.SAL_SUP_ELM_COD  AND
               TMP_FLX_SRC_SCE.IND_GRP_COD     = P_FLX_SCE_CFG_IND.IND_ELM_COD AND
               TMP_FLX_SRC_SCE.FLX_IND_ELM_COD = R_FLX_GRP_IND.IND_ELM_COD
          )
          -- Retrieve LC to EUR_CY and EUR_FY current exchange rates
          LEFT OUTER JOIN COP_DSP_CONTROLLING_CLOUD.R_CURRENCY_UNIFY_AGG AS CUR ON 
          (
               TMP_FLX_SRC_SCE.SRC_CUR_COD = CUR.CUR_COD AND
               TMP_FLX_SRC_SCE.CBU_COD     = CUR.CBU_COD
          )
    WHERE  (UNPIVOT_DATA.NUM_VAL  != 0
    OR      UNPIVOT_DATA.DEN_VAL  != 0)
    AND    TMP_FLX_SRC_SCE.AMOUNT != 0
    ;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 4. Drop the Tempoorary table TMP_FLX_SRC_SCE_FLX

    v_STEP_TABLE := 'DROP TEMPORARY TABLE TMP_FLX_SRC_SCE_FLX';
    v_STEP_NUM   := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    DROP TABLE COP_DMT_FLX.TMP_FLX_SRC_SCE_FLX;

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

         -- Assign the end date of the step
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

         -- Call the procedure to log the step in error with the error message
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, -1, :v_ERR_MSG, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         v_ERR_MSG := v_ERR_MSG || ' in the step ' || v_STEP_TABLE;

         v_STEP_TABLE := 'UPDATE STATUS TO failed IN R_FLX_SCE';
         v_STEP_NUM   := v_STEP_NUM + 1;

         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

         -- xx. Update all scenarios init_in_progress to copy_failed in case of failure
         UPDATE COP_DMT_FLX.R_FLX_SCE
         SET    INI_STS_COD = 'failed'
         WHERE  INI_STS_COD NOT IN ('created','requested','done','failed');

         -- Assign the end date of the step
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

         -- Call the procedure to log the step
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         -- Call the procedure to log the end of the process
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

         DROP TABLE IF EXISTS COP_DMT_FLX.TMP_FLX_SRC_SCE_FLX;


         RETURN v_ERR_MSG;
         RAISE; -- Raise the same exception that you are handling.

END;
$$
;
