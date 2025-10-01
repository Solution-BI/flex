USE SCHEMA COP_DMT_FLX;


CREATE OR REPLACE PROCEDURE SP_Flex_Ini_Close_Gap()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to close the GAP in the itialisation process
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 27-11-2024                                                 
=========================================================================
Modified On:     Author:          Description: 

=========================================================================
*/

DECLARE

v_STEP_TABLE     VARCHAR(256);
V_RUN_ID         VARCHAR(256);
v_STEP_NUM       NUMBER(5,0) := 0;
V_STEP_BEG_DT    VARCHAR(50);
V_STEP_END_DT    VARCHAR(50);
v_ERR_MSG        VARCHAR(1000);
v_IS_SCENARIO    INTEGER;

BEGIN

    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Ini_Close_Gap','#','DLT',CURRENT_USER);

    -- 1. Truncate working table 

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'Truncate working table W_FLX_SRC_SCE__GAP';
    v_STEP_NUM := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    TRUNCATE TABLE COP_DMT_FLX.W_FLX_SRC_SCE__GAP;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- 2. Load the Actual data and keep the source scenario for the missing entity & period

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'INSERT INTO W_FLX_SRC_SCE__GAP';
    v_STEP_NUM := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    INSERT INTO COP_DMT_FLX.W_FLX_SRC_SCE__GAP
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
    WITH After_ AS (
        SELECT   CBU_COD
                ,SCE_ELM_COD
                ,CFG_ORD_NUM
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
                ,ACCOUNT_ELEMENT_CODE
                ,DESTINATION_ELEMENT_CODE
                ,FUNCTIONAL_AREA_ELEMENT_CODE
                ,CATEGORY_ELEMENT_CODE
                ,CHANNEL_ELEMENT_CODE
                ,IOM_CODE
                ,PLANT_CODE
                ,ORIGINAL_ACCOUNT_ELEMENT_CODE
                ,SUM(AMOUNT)                          AMOUNT
        FROM     COP_DMT_FLX.W_FLX_SRC_SCE__ACT
        GROUP BY ALL
    )
   ,Before_ AS (
        SELECT   CBU_COD
                ,SCE_ELM_COD
                ,CFG_ORD_NUM
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
                ,ACCOUNT_ELEMENT_CODE
                ,DESTINATION_ELEMENT_CODE
                ,FUNCTIONAL_AREA_ELEMENT_CODE
                ,CATEGORY_ELEMENT_CODE
                ,CHANNEL_ELEMENT_CODE
                ,IOM_CODE
                ,PLANT_CODE
                ,ORIGINAL_ACCOUNT_ELEMENT_CODE
                ,SUM(AMOUNT)                          AMOUNT
        FROM     COP_DMT_FLX.W_FLX_SRC_SCE__FCA_VAR
        GROUP BY ALL
    )
   ,GAP_Values AS (
        SELECT   COALESCE(After_.CBU_COD,Before_.CBU_COD)                                               CBU_COD
                ,COALESCE(After_.SCE_ELM_COD,Before_.SCE_ELM_COD)                                       SCE_ELM_COD
                ,COALESCE(After_.CFG_ORD_NUM,Before_.CFG_ORD_NUM)                                       CFG_ORD_NUM
                ,COALESCE(After_.IND_GRP_COD,Before_.IND_GRP_COD)                                       IND_GRP_COD
                ,COALESCE(After_.IND_GRP_RAT_FLG,Before_.IND_GRP_RAT_FLG)                               IND_GRP_RAT_FLG
                ,COALESCE(After_.BGN_PER_ELM_COD,Before_.BGN_PER_ELM_COD)                               BGN_PER_ELM_COD
                ,COALESCE(After_.END_PER_ELM_COD,Before_.END_PER_ELM_COD)                               END_PER_ELM_COD
                ,COALESCE(After_.ETI_GRP_COD,Before_.ETI_GRP_COD)                                       ETI_GRP_COD
                ,COALESCE(After_.CUS_GRP_COD,Before_.CUS_GRP_COD)                                       CUS_GRP_COD
                ,COALESCE(After_.CUS_DIM_GRP_COD,Before_.CUS_DIM_GRP_COD)                               CUS_DIM_GRP_COD
                ,COALESCE(After_.PDT_GRP_COD,Before_.PDT_GRP_COD)                                       PDT_GRP_COD
                ,COALESCE(After_.CAT_TYP_GRP_COD,Before_.CAT_TYP_GRP_COD)                               CAT_TYP_GRP_COD
                ,COALESCE(After_.EIB_GRP_COD,Before_.EIB_GRP_COD)                                       EIB_GRP_COD
                ,COALESCE(After_.TTY_GRP_COD,Before_.TTY_GRP_COD)                                       TTY_GRP_COD
                ,COALESCE(After_.SAL_SUP_GRP_COD,Before_.SAL_SUP_GRP_COD)                               SAL_SUP_GRP_COD
                ,COALESCE(After_.IND_ELM_COD,Before_.IND_ELM_COD)                                       IND_ELM_COD
                ,COALESCE(After_.CFG_MOD_FLG,Before_.CFG_MOD_FLG)                                       CFG_MOD_FLG
                ,COALESCE(After_.IND_NUM_FLG,Before_.IND_NUM_FLG)                                       IND_NUM_FLG
                ,COALESCE(After_.IND_DEN_FLG,Before_.IND_DEN_FLG)                                       IND_DEN_FLG
                ,COALESCE(After_.ETI_ELM_COD,Before_.ETI_ELM_COD)                                       ETI_ELM_COD
                ,COALESCE(After_.CUS_ELM_COD,Before_.CUS_ELM_COD)                                       CUS_ELM_COD
                ,COALESCE(After_.PDT_ELM_COD,Before_.PDT_ELM_COD)                                       PDT_ELM_COD
                ,COALESCE(After_.CAT_TYP_ELM_COD,Before_.CAT_TYP_ELM_COD)                               CAT_TYP_ELM_COD
                ,COALESCE(After_.EIB_ELM_COD,Before_.EIB_ELM_COD)                                       EIB_ELM_COD
                ,COALESCE(After_.TTY_ELM_COD,Before_.TTY_ELM_COD)                                       TTY_ELM_COD
                ,COALESCE(After_.SAL_SUP_ELM_COD,Before_.SAL_SUP_ELM_COD)                               SAL_SUP_ELM_COD
                ,COALESCE(After_.SRC_CUR_COD,Before_.SRC_CUR_COD)                                       SRC_CUR_COD
                ,COALESCE(After_.SCE_CUR_COD,Before_.SCE_CUR_COD)                                       SCE_CUR_COD
                ,COALESCE(After_.CNV_CUR_RAT,Before_.CNV_CUR_RAT)                                       CNV_CUR_RAT
                ,COALESCE(After_.ACCOUNT_ELEMENT_CODE,Before_.ACCOUNT_ELEMENT_CODE)                     ACCOUNT_ELEMENT_CODE
                ,COALESCE(After_.DESTINATION_ELEMENT_CODE,Before_.DESTINATION_ELEMENT_CODE)             DESTINATION_ELEMENT_CODE
                ,COALESCE(After_.FUNCTIONAL_AREA_ELEMENT_CODE,Before_.FUNCTIONAL_AREA_ELEMENT_CODE)     FUNCTIONAL_AREA_ELEMENT_CODE
                ,COALESCE(After_.CATEGORY_ELEMENT_CODE,Before_.CATEGORY_ELEMENT_CODE)                   CATEGORY_ELEMENT_CODE
                ,COALESCE(After_.CHANNEL_ELEMENT_CODE,Before_.CHANNEL_ELEMENT_CODE)                     CHANNEL_ELEMENT_CODE
                ,COALESCE(After_.IOM_CODE,Before_.IOM_CODE)                                             IOM_CODE
                ,COALESCE(After_.PLANT_CODE,Before_.PLANT_CODE)                                         PLANT_CODE
                ,COALESCE(After_.ORIGINAL_ACCOUNT_ELEMENT_CODE,Before_.ORIGINAL_ACCOUNT_ELEMENT_CODE)   ORIGINAL_ACCOUNT_ELEMENT_CODE
                ,COALESCE(Before_.AMOUNT,0) - COALESCE(After_.AMOUNT,0)                                 gap_amount
        FROM     After_
                 FULL OUTER JOIN Before_ ON
                 (
                     Before_.CBU_COD                        = After_.CBU_COD                       AND
                     Before_.SCE_ELM_COD                    = After_.SCE_ELM_COD                   AND
                     Before_.CFG_ORD_NUM                    = After_.CFG_ORD_NUM                   AND
                     Before_.IND_GRP_COD                    = After_.IND_GRP_COD                   AND
                     Before_.IND_GRP_RAT_FLG                = After_.IND_GRP_RAT_FLG               AND
                     Before_.BGN_PER_ELM_COD                = After_.BGN_PER_ELM_COD               AND
                     Before_.END_PER_ELM_COD                = After_.END_PER_ELM_COD               AND
                     Before_.ETI_GRP_COD                    = After_.ETI_GRP_COD                   AND
                     Before_.CUS_GRP_COD                    = After_.CUS_GRP_COD                   AND
                     Before_.CUS_DIM_GRP_COD                = After_.CUS_DIM_GRP_COD               AND
                     Before_.PDT_GRP_COD                    = After_.PDT_GRP_COD                   AND
                     Before_.CAT_TYP_GRP_COD                = After_.CAT_TYP_GRP_COD               AND
                     Before_.EIB_GRP_COD                    = After_.EIB_GRP_COD                   AND
                     Before_.TTY_GRP_COD                    = After_.TTY_GRP_COD                   AND
                     Before_.SAL_SUP_GRP_COD                = After_.SAL_SUP_GRP_COD               AND
                     Before_.IND_ELM_COD                    = After_.IND_ELM_COD                   AND
                     Before_.IND_NUM_FLG                    = After_.IND_NUM_FLG                   AND
                     Before_.IND_DEN_FLG                    = After_.IND_DEN_FLG                   AND
                     Before_.ETI_ELM_COD                    = After_.ETI_ELM_COD                   AND
                     Before_.CUS_ELM_COD                    = After_.CUS_ELM_COD                   AND
                     Before_.PDT_ELM_COD                    = After_.PDT_ELM_COD                   AND
                     Before_.CAT_TYP_ELM_COD                = After_.CAT_TYP_ELM_COD               AND
                     Before_.EIB_ELM_COD                    = After_.EIB_ELM_COD                   AND
                     Before_.TTY_ELM_COD                    = After_.TTY_ELM_COD                   AND
                     Before_.SAL_SUP_ELM_COD                = After_.SAL_SUP_ELM_COD               AND
                     Before_.SRC_CUR_COD                    = After_.SRC_CUR_COD                   AND
                     Before_.SCE_CUR_COD                    = After_.SCE_CUR_COD                   AND
                     Before_.CNV_CUR_RAT                    = After_.CNV_CUR_RAT                   AND
                     Before_.ACCOUNT_ELEMENT_CODE           = After_.ACCOUNT_ELEMENT_CODE          AND
                     Before_.DESTINATION_ELEMENT_CODE       = After_.DESTINATION_ELEMENT_CODE      AND
                     Before_.FUNCTIONAL_AREA_ELEMENT_CODE   = After_.FUNCTIONAL_AREA_ELEMENT_CODE  AND
                     Before_.CATEGORY_ELEMENT_CODE          = After_.CATEGORY_ELEMENT_CODE         AND
                     Before_.CHANNEL_ELEMENT_CODE           = After_.CHANNEL_ELEMENT_CODE          AND
                     Before_.IOM_CODE                       = After_.IOM_CODE                      AND
                     Before_.PLANT_CODE                     = After_.PLANT_CODE                    AND
                     Before_.ORIGINAL_ACCOUNT_ELEMENT_CODE  = After_.ORIGINAL_ACCOUNT_ELEMENT_CODE
                 )
        WHERE    COALESCE(Before_.AMOUNT,0) - COALESCE(After_.AMOUNT,0) != 0
    )
    SELECT  GAP_Values.CBU_COD
           ,GAP_Values.SCE_ELM_COD
           ,GAP_Values.CFG_ORD_NUM
           ,'CLOSE_GAP'                               SRC_SCE_ELM_COD
           ,GAP_Values.IND_GRP_COD
           ,GAP_Values.IND_GRP_RAT_FLG
           ,GAP_Values.BGN_PER_ELM_COD
           ,GAP_Values.END_PER_ELM_COD
           ,GAP_Values.ETI_GRP_COD
           ,GAP_Values.CUS_GRP_COD
           ,GAP_Values.CUS_DIM_GRP_COD
           ,GAP_Values.PDT_GRP_COD
           ,GAP_Values.CAT_TYP_GRP_COD
           ,GAP_Values.EIB_GRP_COD
           ,GAP_Values.TTY_GRP_COD
           ,GAP_Values.SAL_SUP_GRP_COD
           ,GAP_Values.IND_ELM_COD
           ,GAP_Values.CFG_MOD_FLG
           ,GAP_Values.IND_NUM_FLG
           ,GAP_Values.IND_DEN_FLG
           ,P_FLX_GAP_CLO_CFG.GAP_CLO_PER_COD                                                 PER_ELM_COD
           ,(CASE WHEN P_FLX_GAP_CLO_CFG.GAP_CLO_PER_COD <= R_FLX_SCE.LST_ACT_PER_COD THEN TRUE
                  ELSE FALSE 
             END)                                                                             ACT_PER_FLG
           ,GAP_Values.ETI_ELM_COD
           ,GAP_Values.CUS_ELM_COD
           ,GAP_Values.PDT_ELM_COD
           ,GAP_Values.CAT_TYP_ELM_COD
           ,GAP_Values.EIB_ELM_COD
           ,GAP_Values.TTY_ELM_COD
           ,GAP_Values.SAL_SUP_ELM_COD
           ,GAP_Values.SRC_CUR_COD
           ,GAP_Values.SCE_CUR_COD
           ,GAP_Values.CNV_CUR_RAT
           ,1                                                                                 RTO_DEN_AMT
           ,1                                                                                 BAS_DEN_AMT
           ,gap_amount                                                                        AMOUNT
           ,GAP_Values.ACCOUNT_ELEMENT_CODE
           ,GAP_Values.DESTINATION_ELEMENT_CODE
           ,GAP_Values.FUNCTIONAL_AREA_ELEMENT_CODE
           ,GAP_Values.CATEGORY_ELEMENT_CODE
           ,GAP_Values.CHANNEL_ELEMENT_CODE
           ,GAP_Values.IOM_CODE
           ,GAP_Values.PLANT_CODE
           ,GAP_Values.ORIGINAL_ACCOUNT_ELEMENT_CODE
           ,CURRENT_TIMESTAMP                                                                 T_REC_SRC_TST
           ,CURRENT_TIMESTAMP                                                                 T_REC_INS_TST
           ,CURRENT_TIMESTAMP                                                                 T_REC_UPD_TST
    FROM    GAP_Values
            INNER JOIN COP_DMT_FLX.R_FLX_PDT ON
            (
                 GAP_Values.CBU_COD     = R_FLX_PDT.CBU_COD     AND
                 GAP_Values.PDT_ELM_COD = R_FLX_PDT.PDT_ELM_COD
            )
            INNER JOIN COP_DMT_FLX.P_FLX_GAP_CLO_CFG ON
            (
                 GAP_Values.CBU_COD        = P_FLX_GAP_CLO_CFG.CBU_COD         AND
                 GAP_Values.SCE_ELM_COD    = P_FLX_GAP_CLO_CFG.SCE_ELM_COD     AND
                 GAP_Values.ETI_ELM_COD    = P_FLX_GAP_CLO_CFG.ETI_ELM_COD     AND
                 R_FLX_PDT.LV0_PDT_CAT_COD = P_FLX_GAP_CLO_CFG.LV0_PDT_CAT_COD
            )
            INNER JOIN COP_DMT_FLX.R_FLX_SCE ON
            (
                 R_FLX_SCE.CBU_COD      = GAP_Values.CBU_COD     AND
                 R_FLX_SCE.SCE_ELM_COD  = GAP_Values.SCE_ELM_COD AND
                 R_FLX_SCE.UPD_ACT_FLG  = 1
            )
    UNION ALL
    SELECT  W_FLX_SRC_SCE__ACT.CBU_COD
           ,W_FLX_SRC_SCE__ACT.SCE_ELM_COD
           ,W_FLX_SRC_SCE__ACT.CFG_ORD_NUM
           ,W_FLX_SRC_SCE__ACT.SRC_SCE_ELM_COD
           ,W_FLX_SRC_SCE__ACT.IND_GRP_COD
           ,W_FLX_SRC_SCE__ACT.IND_GRP_RAT_FLG
           ,W_FLX_SRC_SCE__ACT.BGN_PER_ELM_COD
           ,W_FLX_SRC_SCE__ACT.END_PER_ELM_COD
           ,W_FLX_SRC_SCE__ACT.ETI_GRP_COD
           ,W_FLX_SRC_SCE__ACT.CUS_GRP_COD
           ,W_FLX_SRC_SCE__ACT.CUS_DIM_GRP_COD
           ,W_FLX_SRC_SCE__ACT.PDT_GRP_COD
           ,W_FLX_SRC_SCE__ACT.CAT_TYP_GRP_COD
           ,W_FLX_SRC_SCE__ACT.EIB_GRP_COD
           ,W_FLX_SRC_SCE__ACT.TTY_GRP_COD
           ,W_FLX_SRC_SCE__ACT.SAL_SUP_GRP_COD
           ,W_FLX_SRC_SCE__ACT.IND_ELM_COD
           ,W_FLX_SRC_SCE__ACT.CFG_MOD_FLG
           ,W_FLX_SRC_SCE__ACT.IND_NUM_FLG
           ,W_FLX_SRC_SCE__ACT.IND_DEN_FLG
           ,W_FLX_SRC_SCE__ACT.PER_ELM_COD
           ,W_FLX_SRC_SCE__ACT.ACT_PER_FLG
           ,W_FLX_SRC_SCE__ACT.ETI_ELM_COD
           ,W_FLX_SRC_SCE__ACT.CUS_ELM_COD
           ,W_FLX_SRC_SCE__ACT.PDT_ELM_COD
           ,W_FLX_SRC_SCE__ACT.CAT_TYP_ELM_COD
           ,W_FLX_SRC_SCE__ACT.EIB_ELM_COD
           ,W_FLX_SRC_SCE__ACT.TTY_ELM_COD
           ,W_FLX_SRC_SCE__ACT.SAL_SUP_ELM_COD
           ,W_FLX_SRC_SCE__ACT.SRC_CUR_COD
           ,W_FLX_SRC_SCE__ACT.SCE_CUR_COD
           ,W_FLX_SRC_SCE__ACT.CNV_CUR_RAT
           ,W_FLX_SRC_SCE__ACT.RTO_DEN_AMT
           ,W_FLX_SRC_SCE__ACT.BAS_DEN_AMT
           ,W_FLX_SRC_SCE__ACT.AMOUNT
           ,W_FLX_SRC_SCE__ACT.ACCOUNT_ELEMENT_CODE
           ,W_FLX_SRC_SCE__ACT.DESTINATION_ELEMENT_CODE
           ,W_FLX_SRC_SCE__ACT.FUNCTIONAL_AREA_ELEMENT_CODE
           ,W_FLX_SRC_SCE__ACT.CATEGORY_ELEMENT_CODE
           ,W_FLX_SRC_SCE__ACT.CHANNEL_ELEMENT_CODE
           ,W_FLX_SRC_SCE__ACT.IOM_CODE
           ,W_FLX_SRC_SCE__ACT.PLANT_CODE
           ,W_FLX_SRC_SCE__ACT.ORIGINAL_ACCOUNT_ELEMENT_CODE
           ,W_FLX_SRC_SCE__ACT.T_REC_SRC_TST
           ,W_FLX_SRC_SCE__ACT.T_REC_INS_TST
           ,CURRENT_TIMESTAMP                                                                 T_REC_UPD_TST
    FROM    COP_DMT_FLX.W_FLX_SRC_SCE__ACT;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- Call the procedure to log the end of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);

    -- 4. Return text when the stored procedure finish in success
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

         -- xx. Update all scenarios to failed in case of failure
         UPDATE COP_DMT_FLX.R_FLX_SCE
         SET    INI_STS_COD = 'failed'
         WHERE  INI_STS_COD NOT IN ('created','done','failed','requested');

         -- Assign the end date of the step
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

         -- Call the procedure to log the step
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

         -- Call the procedure to log the end of the process
         CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);
        
         RETURN v_ERR_MSG;
         RAISE; -- Raise the same exception that you are handling.

END;
$$
;
