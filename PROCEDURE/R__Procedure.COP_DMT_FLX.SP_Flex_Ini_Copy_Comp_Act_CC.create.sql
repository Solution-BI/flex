USE DATABASE {{env}}_COP;
USE SCHEMA COP_DMT_FLX;


CREATE OR REPLACE PROCEDURE SP_Flex_Ini_Copy_Comp_Act_CC()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$

/*
=========================================================================
                                          HISTORY
=========================================================================
Description : Procedure Script For Copy Comparables & Actual Scenario From CC

Author      : Noël COQUIO (Solution BI France)
Created On  : 03-12-2024
=========================================================================
Modified On:     Description:                                Author:
=========================================================================
*/

DECLARE
v_STEP_TABLE      VARCHAR(256);
V_RUN_ID          VARCHAR(256);
v_STEP_NUM        NUMBER(5,0) := 0;
V_STEP_BEG_DT     VARCHAR(50);
V_STEP_END_DT     VARCHAR(50);
v_ERR_MSG         VARCHAR(1000);

BEGIN
	
    -- Generate the UUID for the procedure
    CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

    -- Call the procedure to log the init of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Ini_Copy_Comp_Act_CC','#','DLT',CURRENT_USER);

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'TRUNCATE W_FLX_CMP_SCE';
    v_STEP_NUM := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    -- 1. Truncate the comparable scenario working table for the scenario

    TRUNCATE TABLE COP_DMT_FLX.W_FLX_CMP_SCE;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);
	
    -- 2. Insert Comparable & Actual scenario into W_FLX_CMP_SCE

    -- Assign the step name, step num and start date of the step
    v_STEP_TABLE := 'INSERT COMPARABLE & ACTUAL SCENARIO INTO W_FLX_CMP_SCE';
    v_STEP_NUM := v_STEP_NUM + 1;

    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

    INSERT INTO COP_DMT_FLX.W_FLX_CMP_SCE
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
               --,AMOUNT_EUR
               ,CUR_COD
               ,T_REC_SRC_TST
               ,T_REC_INS_TST
               ,T_REC_UPD_TST
               )
    WITH FLX_SCE as (
        SELECT DISTINCT 
               CBU_COD
              ,SRC_SCE_ELM_COD
        FROM   COP_DMT_FLX.R_FLX_SCE
               UNPIVOT (SRC_SCE_ELM_COD FOR COLNAME IN (CMP_1ST_SRC_SCE_COD
                                                       ,CMP_2ND_SRC_SCE_COD
                                                       ,CMP_3RD_SRC_SCE_COD))
        WHERE  R_FLX_SCE.INI_STS_COD NOT IN ('created','done','failed','requested')
        UNION
        /* Add the Actual scenario only if the update actual flag is filled */
        SELECT DISTINCT 
               CBU_COD
              ,ACT_SRC_SCE_COD SRC_SCE_ELM_COD
        FROM   COP_DMT_FLX.R_FLX_SCE
        WHERE  R_FLX_SCE.INI_STS_COD NOT IN ('created','done','failed','requested')
        AND    R_FLX_SCE.UPD_ACT_FLG      = 1
    )
    SELECT      cc.CBU_COD
               ,cc.SCENARIO_TYPE_CODE
               ,cc.SCENARIO_ELEMENT_CODE
               ,cc.RVN_DATE
               ,cc.SCENARIO_DATE
               ,cc.PERIOD_ELEMENT_CODE
               ,cc.ENTITY_ELEMENT_CODE
               ,cc.ACCOUNT_ELEMENT_CODE
               ,cc.DESTINATION_ELEMENT_CODE
               ,cc.FCT_ARE
               ,cc.SU_SP_SPLIT_CODE
               ,REPLACE(cc.CUSTOMER_ELEMENT_CODE,'PLN_','POL_') CUSTOMER_ELEMENT_CODE
               ,cc.PRODUCT_ELEMENT_CODE
               --,cc.INNO_RENO_KEY_CODE  -- Not used in Flex, will be recalculated from the other elements
               --,cc.PRODUCT_ATTRIBUTE_KEY_CODE  -- Not used in Flex, will be recalculated from the other elements
               ,cc.CATEGORY_ELEMENT_CODE
               ,cc.MGR_L500_CODE
               ,cc.EIB_ELEMENT_CODE
               ,cc.TERRITORY_ELEMENT_CODE
               ,CAST('NA' AS varchar(30))                    as channel_element_code  -- Not used in Flex, writeback will be done using NA
               ,CAST('NA' AS varchar(30))                    as iom_code  -- Not used in Flex, writeback will be done using NA
               ,CAST('NA' AS varchar(30))                    as plant_code  -- Not used in Flex, writeback will be done using NA
               ,cc.ORIGINAL_ACCOUNT_ELEMENT_CODE
               ,cc.IND_ELM_COD                               as indicator_code
               ,cc.CFG_MOD_FLG
               ,cc.RATE_CY
               ,cc.RATE_FY
               ,cc.AMOUNT_LC
               ,cc.AMOUNT_EUR_CY
               ,cc.AMOUNT_EUR_FY
               --,cc.AMOUNT_EUR  -- Not used in Flex
               ,cc.CUR_COD
               ,cc.T_UPD_TST                                 as t_rec_src_tst
               ,current_timestamp                            as t_rec_ins_tst
               ,current_timestamp                            as t_rec_upd_tst
    FROM        COP_DSP_CONTROLLING_CLOUD.F_CCD_FULL_DTL_UNIFY as cc
                INNER JOIN FLX_SCE ON
                (
                     FLX_SCE.CBU_COD         = cc.CBU_COD               AND
                     FLX_SCE.SRC_SCE_ELM_COD = cc.SCENARIO_ELEMENT_CODE
                )
    ;

    -- Assign the end date of the step
    SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

    -- Call the procedure to log the step
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

    -- Call the procedure to log the end of the process
    CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);
    
    -- 3. Return text when the stored procedure finish in success
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
         v_STEP_NUM   := v_STEP_NUM + 1;
          
         SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

         UPDATE COP_DMT_FLX.R_FLX_SCE
         SET     INI_STS_COD = 'failed'
         WHERE  INI_STS_COD NOT IN ('created','done','failed','requested');

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
