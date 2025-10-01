USE DATABASE {{env}}_COP;
USE SCHEMA COP_DMT_FLX;

/*
=========================================================================
                                          HISTORY
=========================================================================
DESCRIPTION : CREATE PROCEDURE SCRIPT FOR COPY SCNEARIO FROM CC TO W_FLX_SRC_SCE 

AUTHOR        : Noel Coquio (SOLUTION BI FRANCE)
CREATED ON  : 27-11-2024
=========================================================================
MODIFIED ON:     DESCRIPTION:                                AUTHOR:
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Ini_Calc_from_Ratios()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
DECLARE
v_STEP_TABLE      VARCHAR(256);
V_RUN_ID          VARCHAR(256);
v_STEP_NUM        NUMBER(5,0) := 0;
V_STEP_BEG_DT     VARCHAR(50);
V_STEP_END_DT     VARCHAR(50);
v_ERR_MSG         VARCHAR(1000);
v_IS_SCENARIO     INTEGER;

BEGIN

    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Ini_Calc_from_Ratios','#','DLT',CURRENT_USER);

    -- 1. Truncate working table 

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'Truncate working table W_FLX_SRC_SCE__CLC';
    v_STEP_NUM := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SRC_SCE__CLC;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 2. Retrieve missing Source scenarios data at lowest level, will be used for disaggregation

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'INSERT INTO W_FLX_SRC_SCE__CLC';
    v_STEP_NUM := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    INSERT INTO COP_DMT_FLX.W_FLX_SRC_SCE__CLC
               (CBU_COD
               ,SCE_ELM_COD
               ,CFG_ORD_NUM
               ,SRC_SCE_ELM_COD
               ,IND_GRP_COD
               ,IND_GRP_RAT_FLG
               ,BGN_PER_ELM_COD
               ,END_PER_ELM_COD
               ,ETI_GRP_COD
               ,CUS_GRP_COD
               ,CUS_DIM_GRP_COD
               ,PDT_GRP_COD
               ,CAT_TYP_GRP_COD
               ,EIB_GRP_COD
               ,TTY_GRP_COD
               ,SAL_SUP_GRP_COD
               ,IND_ELM_COD
               ,CFG_MOD_FLG
               ,IND_NUM_FLG
               ,IND_DEN_FLG
               ,PER_ELM_COD
               ,ACT_PER_FLG
               ,ETI_ELM_COD
               ,CUS_ELM_COD
               ,PDT_ELM_COD
               ,CAT_TYP_ELM_COD
               ,EIB_ELM_COD
               ,TTY_ELM_COD
               ,SAL_SUP_ELM_COD
               ,SRC_CUR_COD
               ,SCE_CUR_COD
               ,CNV_CUR_RAT
               ,RTO_DEN_AMT
               ,BAS_DEN_AMT
               ,AMOUNT
               ,ACCOUNT_ELEMENT_CODE
               ,DESTINATION_ELEMENT_CODE
               ,FUNCTIONAL_AREA_ELEMENT_CODE
               ,CATEGORY_ELEMENT_CODE
               ,CHANNEL_ELEMENT_CODE
               ,IOM_CODE
               ,PLANT_CODE
               ,ORIGINAL_ACCOUNT_ELEMENT_CODE
               ,T_REC_SRC_TST
               ,T_REC_INS_TST
               ,T_REC_UPD_TST  
               )
    WITH RATIO_DEN AS (
    -- CALCULATE THE DENOMINATOR FROM THE RATIO SOURCE SCENARIO
         SELECT   CBU_COD
                 ,SCE_ELM_COD
                 ,IND_GRP_COD
                 ,IND_ELM_COD
                 ,PER_ELM_COD
                 ,ETI_ELM_COD
                 ,CUS_ELM_COD
                 ,PDT_ELM_COD
                 ,CAT_TYP_ELM_COD
                 ,EIB_ELM_COD
                 ,TTY_ELM_COD
                 ,SAL_SUP_ELM_COD
                 ,SRC_CUR_COD
                 ,SUM(AMOUNT) AS RATIO_DEN_AMOUNT
         FROM     W_FLX_SRC_SCE__FLT
         WHERE    IND_DEN_FLG = 1
         GROUP BY ALL
         HAVING    SUM(AMOUNT) <> 0
    )
   ,BASE_DEN AS (
    -- CALCULATE ALL BASE INDICATORS AT THE FLEX SCENARIO LEVEL
    -- NOTE: THERE ARE NOT ONLY DENOMINATORS FROM THE RATIO SOURCE SCENARIO,
    -- TO KEEP THE QUERY SIMPLE. THOSE WILL BE FILTERED IN THE NEXT STEP.
         SELECT   CBU_COD
                 ,SCE_ELM_COD
                 ,IND_ELM_COD
                 ,PER_ELM_COD
                 ,ETI_ELM_COD
                 ,CUS_ELM_COD
                 ,PDT_ELM_COD
                 ,CAT_TYP_ELM_COD
                 ,EIB_ELM_COD
                 ,TTY_ELM_COD
                 ,SAL_SUP_ELM_COD
                 ,SRC_CUR_COD
                 ,SUM(AMOUNT) AS BASE_DEN_AMOUNT
         FROM     W_FLX_SRC_SCE__FLT
         WHERE    IND_NUM_FLG = 1
         GROUP BY ALL
         HAVING    SUM(AMOUNT) <> 0
    )
   ,DEN AS (
    -- MATCH THE DENOMINATOR FROM THE RATIO SOURCE SCENARIO AND THE INDICATOR SOURCE SCENARIO
    -- NOTE: IF THE DENOMINATOR IS NULL (OR ZERO, OR MISSING) FOR ANY SOURCE SCENARIO,
    -- IT WILL BE IGNORED AND THE TARGET KPI (I.E. THE NUMERATOR OF THE RATIO) WILL BE ZERO
    -- IN THE TARGET SCENARIO
         SELECT   RATIO_DEN.CBU_COD
                 ,RATIO_DEN.SCE_ELM_COD
                 ,RATIO_DEN.IND_GRP_COD  -- RATIO
                 ,RATIO_DEN.PER_ELM_COD
                 ,RATIO_DEN.ETI_ELM_COD
                 ,RATIO_DEN.CUS_ELM_COD
                 ,RATIO_DEN.PDT_ELM_COD
                 ,RATIO_DEN.CAT_TYP_ELM_COD
                 ,RATIO_DEN.EIB_ELM_COD
                 ,RATIO_DEN.TTY_ELM_COD
                 ,RATIO_DEN.SAL_SUP_ELM_COD
                 ,RATIO_DEN.SRC_CUR_COD
                 ,COALESCE(NULLIFZERO(RATIO_DEN.RATIO_DEN_AMOUNT),1) AS RATIO_DEN_AMOUNT
                 ,SUM(BASE_DEN.BASE_DEN_AMOUNT)    AS BASE_DEN_AMOUNT
         FROM     RATIO_DEN
                  INNER JOIN BASE_DEN ON 
                  (
                       RATIO_DEN.CBU_COD         = BASE_DEN.CBU_COD         AND
                       RATIO_DEN.SCE_ELM_COD     = BASE_DEN.SCE_ELM_COD     AND
                       RATIO_DEN.IND_ELM_COD     = BASE_DEN.IND_ELM_COD     AND
                       RATIO_DEN.PER_ELM_COD     = BASE_DEN.PER_ELM_COD     AND
                       RATIO_DEN.ETI_ELM_COD     = BASE_DEN.ETI_ELM_COD     AND
                       RATIO_DEN.CUS_ELM_COD     = BASE_DEN.CUS_ELM_COD     AND
                       RATIO_DEN.PDT_ELM_COD     = BASE_DEN.PDT_ELM_COD     AND
                       RATIO_DEN.CAT_TYP_ELM_COD = BASE_DEN.CAT_TYP_ELM_COD AND
                       RATIO_DEN.EIB_ELM_COD     = BASE_DEN.EIB_ELM_COD     AND
                       RATIO_DEN.TTY_ELM_COD     = BASE_DEN.TTY_ELM_COD     AND
                       RATIO_DEN.SAL_SUP_ELM_COD = BASE_DEN.SAL_SUP_ELM_COD AND
                       RATIO_DEN.SRC_CUR_COD     = BASE_DEN.SRC_CUR_COD
                  )
         GROUP BY ALL
    )
    -- FOR RATIOS, THE NUMERATOR IS RECALCULATED FROM THE RATIO'S AND THE DENOMINATOR'S SOURCE SCENARIOS
    -- THE RATIO IS APPLIED TO ALL BASE KPIS, IF THE NUMERATOR IS COMPOSED
    SELECT   W_FLX_SRC_SCE__FLT.CBU_COD
            ,W_FLX_SRC_SCE__FLT.SCE_ELM_COD
            ,W_FLX_SRC_SCE__FLT.CFG_ORD_NUM
            ,W_FLX_SRC_SCE__FLT.SRC_SCE_ELM_COD
            ,W_FLX_SRC_SCE__FLT.IND_GRP_COD
            ,W_FLX_SRC_SCE__FLT.IND_GRP_RAT_FLG
            ,W_FLX_SRC_SCE__FLT.BGN_PER_ELM_COD
            ,W_FLX_SRC_SCE__FLT.END_PER_ELM_COD
            ,W_FLX_SRC_SCE__FLT.ETI_GRP_COD
            ,W_FLX_SRC_SCE__FLT.CUS_GRP_COD
            ,W_FLX_SRC_SCE__FLT.CUS_DIM_GRP_COD
            ,W_FLX_SRC_SCE__FLT.PDT_GRP_COD
            ,W_FLX_SRC_SCE__FLT.CAT_TYP_GRP_COD
            ,W_FLX_SRC_SCE__FLT.EIB_GRP_COD
            ,W_FLX_SRC_SCE__FLT.TTY_GRP_COD
            ,W_FLX_SRC_SCE__FLT.SAL_SUP_GRP_COD
            ,W_FLX_SRC_SCE__FLT.IND_ELM_COD
            ,W_FLX_SRC_SCE__FLT.CFG_MOD_FLG
            ,W_FLX_SRC_SCE__FLT.IND_NUM_FLG
            ,W_FLX_SRC_SCE__FLT.IND_DEN_FLG
            ,W_FLX_SRC_SCE__FLT.PER_ELM_COD
            ,W_FLX_SRC_SCE__FLT.ACT_PER_FLG
            ,W_FLX_SRC_SCE__FLT.ETI_ELM_COD
            ,W_FLX_SRC_SCE__FLT.CUS_ELM_COD
            ,W_FLX_SRC_SCE__FLT.PDT_ELM_COD
            ,W_FLX_SRC_SCE__FLT.CAT_TYP_ELM_COD
            ,W_FLX_SRC_SCE__FLT.EIB_ELM_COD
            ,W_FLX_SRC_SCE__FLT.TTY_ELM_COD
            ,W_FLX_SRC_SCE__FLT.SAL_SUP_ELM_COD
            ,W_FLX_SRC_SCE__FLT.SRC_CUR_COD
            ,W_FLX_SRC_SCE__FLT.SCE_CUR_COD
            ,W_FLX_SRC_SCE__FLT.CNV_CUR_RAT
            ,DEN.RATIO_DEN_AMOUNT
            ,DEN.BASE_DEN_AMOUNT
            ,ROUND(W_FLX_SRC_SCE__FLT.AMOUNT / COALESCE(NULLIFZERO(DEN.RATIO_DEN_AMOUNT),1) * DEN.BASE_DEN_AMOUNT, 15) AS AMOUNT
            ,W_FLX_SRC_SCE__FLT.ACCOUNT_ELEMENT_CODE
            ,W_FLX_SRC_SCE__FLT.DESTINATION_ELEMENT_CODE
            ,W_FLX_SRC_SCE__FLT.FUNCTIONAL_AREA_ELEMENT_CODE
            ,W_FLX_SRC_SCE__FLT.CATEGORY_ELEMENT_CODE
            ,W_FLX_SRC_SCE__FLT.CHANNEL_ELEMENT_CODE
            ,W_FLX_SRC_SCE__FLT.IOM_CODE
            ,W_FLX_SRC_SCE__FLT.PLANT_CODE
            ,W_FLX_SRC_SCE__FLT.ORIGINAL_ACCOUNT_ELEMENT_CODE
            ,W_FLX_SRC_SCE__FLT.T_REC_SRC_TST
            ,W_FLX_SRC_SCE__FLT.T_REC_INS_TST
            ,TO_TIMESTAMP(CURRENT_TIMESTAMP)
    FROM    W_FLX_SRC_SCE__FLT
            INNER JOIN DEN ON 
            (
                 W_FLX_SRC_SCE__FLT.CBU_COD         = DEN.CBU_COD         AND
                 W_FLX_SRC_SCE__FLT.SCE_ELM_COD     = DEN.SCE_ELM_COD     AND
                 W_FLX_SRC_SCE__FLT.IND_GRP_COD     = DEN.IND_GRP_COD     AND  -- THE RATIO WILL BE APPLIED TO ALL COMPONENTS
                 W_FLX_SRC_SCE__FLT.PER_ELM_COD     = DEN.PER_ELM_COD     AND
                 W_FLX_SRC_SCE__FLT.ETI_ELM_COD     = DEN.ETI_ELM_COD     AND
                 W_FLX_SRC_SCE__FLT.CUS_ELM_COD     = DEN.CUS_ELM_COD     AND
                 W_FLX_SRC_SCE__FLT.PDT_ELM_COD     = DEN.PDT_ELM_COD     AND
                 W_FLX_SRC_SCE__FLT.CAT_TYP_ELM_COD = DEN.CAT_TYP_ELM_COD AND
                 W_FLX_SRC_SCE__FLT.EIB_ELM_COD     = DEN.EIB_ELM_COD     AND
                 W_FLX_SRC_SCE__FLT.TTY_ELM_COD     = DEN.TTY_ELM_COD     AND
                 W_FLX_SRC_SCE__FLT.SAL_SUP_ELM_COD = DEN.SAL_SUP_ELM_COD
              )
    WHERE   W_FLX_SRC_SCE__FLT.IND_GRP_RAT_FLG = 1 
    AND     W_FLX_SRC_SCE__FLT.IND_NUM_FLG     = 1

    UNION ALL
    -- NON-RATIO INDICATORS ARE RETRIEVED THE STANDARD WAY
    SELECT   CBU_COD
            ,SCE_ELM_COD
            ,CFG_ORD_NUM
            ,SRC_SCE_ELM_COD
            ,IND_GRP_COD
            ,IND_GRP_RAT_FLG
            ,BGN_PER_ELM_COD
            ,END_PER_ELM_COD
            ,ETI_GRP_COD
            ,CUS_GRP_COD
            ,CUS_DIM_GRP_COD
            ,PDT_GRP_COD
            ,CAT_TYP_GRP_COD
            ,EIB_GRP_COD
            ,TTY_GRP_COD
            ,SAL_SUP_GRP_COD
            ,IND_ELM_COD
            ,CFG_MOD_FLG
            ,IND_NUM_FLG
            ,IND_DEN_FLG
            ,PER_ELM_COD
            ,ACT_PER_FLG
            ,ETI_ELM_COD
            ,CUS_ELM_COD
            ,PDT_ELM_COD
            ,CAT_TYP_ELM_COD
            ,EIB_ELM_COD
            ,TTY_ELM_COD
            ,SAL_SUP_ELM_COD
            ,SRC_CUR_COD
            ,SCE_CUR_COD
            ,CNV_CUR_RAT
            ,1                                        AS RATIO_DEN_AMOUNT
            ,1                                        AS BASE_DEN_AMOUNT
            ,AMOUNT
            ,ACCOUNT_ELEMENT_CODE
            ,DESTINATION_ELEMENT_CODE
            ,FUNCTIONAL_AREA_ELEMENT_CODE
            ,CATEGORY_ELEMENT_CODE
            ,CHANNEL_ELEMENT_CODE
            ,IOM_CODE
            ,PLANT_CODE
            ,ORIGINAL_ACCOUNT_ELEMENT_CODE
            ,T_REC_SRC_TST
            ,T_REC_INS_TST
            ,TO_TIMESTAMP(CURRENT_TIMESTAMP)
    FROM     W_FLX_SRC_SCE__FLT
    WHERE    IND_GRP_RAT_FLG = 0;

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

         -- xx. Update all scenarios to failed in case of failure

         v_STEP_TABLE := 'UPDATE STATUS TO failed IN R_FLX_SCE';
         v_STEP_NUM    := v_STEP_NUM + 1;
         
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

         UPDATE COP_DMT_FLX.R_FLX_SCE
         SET     INI_STS_COD = 'failed'
         WHERE  INI_STS_COD  NOT IN ('created','done','failed','requested');

         -- Assign the end date of the step
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

         -- Call the procedure to log the step
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         -- Call the procedure to log the step
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);
         
         
         RETURN v_ERR_MSG;
         RAISE; -- Raise the same exception that you are handling.

END;
$$
;