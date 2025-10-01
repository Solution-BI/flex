USE DATABASE {{env}}_COP;
USE SCHEMA COP_DMT_FLX;

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Copy Scenario From CC to f_flx_cmp_sce 

Author      : Yanis MOHAMMEDI (Solution BI France)
Created On  : 22-08-2024
=========================================================================
Modified On:    Description:                        Author:
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Scenario_Ini_Copy_Comparable_Scenario()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$

DECLARE
v_STEP_TABLE     VARCHAR(256);
V_RUN_ID         VARCHAR(256);
v_STEP_NUM       NUMBER(5,0);
V_STEP_BEG_DT    VARCHAR(50);
V_STEP_END_DT    VARCHAR(50);
v_ERR_MSG        VARCHAR(1000);

BEGIN
	
   -- Generate the UUID for the procedure
   CALL COP_DMT_FLX.SP_FLEX_GENERATE_UUID() INTO :V_RUN_ID;

   -- Call the procedure to log the init of the process
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_INIT(:V_RUN_ID,'COP_DMT_FLX','SP_Flex_Scenario_Ini_Copy_Comparable_Scenario','#','DLT',CURRENT_USER);

   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'UPDATE R_FLX_SCE FROM in_progress:1 TO in_progress:2';
   v_STEP_NUM := 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

   UPDATE COP_DMT_FLX.R_FLX_SCE
   SET    INI_STS_COD = 'in_progress:2'
   WHERE  INI_STS_COD = 'in_progress:1';

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'DELETE F_FLX_CMP_SCE';
   v_STEP_NUM := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

	-- 1. Retrieve missing Source scenarios data from Controlling Cloud (copy at lowest level, will be used for disaggregation)

    DELETE FROM COP_DMT_FLX.F_FLX_CMP_SCE
    WHERE  (CBU_COD, SCE_ELM_COD, SRC_SCE_ELM_COD) IN (
      SELECT DISTINCT
             CBU_COD
            ,SCE_ELM_COD
            ,SRC_SCE_ELM_COD
      FROM   COP_DMT_FLX.R_FLX_SCE
             UNPIVOT (SRC_SCE_ELM_COD FOR COLNAME IN (CMP_1ST_SRC_SCE_COD
                                                     ,CMP_2ND_SRC_SCE_COD
                                                     ,CMP_3RD_SRC_SCE_COD))
      WHERE  SRC_SCE_ELM_COD <> ''
      AND    SRC_SCE_ELM_COD IS NOT NULL
      AND    INI_STS_COD      = 'in_progress:2'
   );

   -- Assign the end date of the step
   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

   -- Call the procedure to log the step
   CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);
	
   -- 2. Insert Comparable scenario into F_FLX_CMP_SCE

   -- Assign the step name, step num and start date of the step
   v_STEP_TABLE := 'INSERT COMPARABLE SCENARIO INTO F_FLX_CMP_SCE';
   v_STEP_NUM := v_STEP_NUM + 1;

   SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

	INSERT INTO F_FLX_CMP_SCE
	(
        CBU_COD
      , SCE_ELM_COD
      , CFG_ORD_NUM
      , SRC_SCE_ELM_COD
      , IND_GRP_COD
      , IND_GRP_RAT_FLG
      , BGN_PER_ELM_COD
      , END_PER_ELM_COD
      , ETI_GRP_COD
      , CUS_GRP_COD
      , CUS_DIM_GRP_COD
      , PDT_GRP_COD
      , CAT_TYP_GRP_COD
      , EIB_GRP_COD
      , TTY_GRP_COD
      , SAL_SUP_GRP_COD
      , IND_ELM_COD
      , IND_NUM_FLG
      , IND_DEN_FLG
      , PER_ELM_COD
      , ACT_PER_FLG
      , ETI_ELM_COD
      , CUS_ELM_COD
      , PDT_ELM_COD
      , CAT_TYP_ELM_COD
      , EIB_ELM_COD
      , TTY_ELM_COD
      , SAL_SUP_ELM_COD
      , SRC_CUR_COD
      , SCE_CUR_COD
      , CNV_CUR_RAT
      , RTO_DEN_AMT
      , BAS_DEN_AMT
      , AMOUNT
      , ACCOUNT_ELEMENT_CODE
      , DESTINATION_ELEMENT_CODE
      , FUNCTIONAL_AREA_ELEMENT_CODE
      , CATEGORY_ELEMENT_CODE
      , CHANNEL_ELEMENT_CODE
      , IOM_CODE
      , PLANT_CODE
      , T_REC_SRC_TST
      , T_REC_INS_TST
      , T_REC_UPD_TST
	)
WITH FINAL AS (

SELECT   R_FLX_SCE.CBU_COD
        ,R_FLX_SCE.SCE_ELM_COD
        ,P_FLX_SCE_CFG_IND.SCE_ELM_KEY
        ,1                                                      AS CFG_ORD_NUM
        ,R_FLX_SCE.SRC_SCE_ELM_COD
        ,P_FLX_SCE_CFG_IND.IND_ELM_COD                          AS IND_GRP_COD
        ,R_FLX_GRP_IND.IND_GRP_RAT_FLG
        ,P_FLX_SCE_CFG_IND.BGN_PER_ELM_COD
        ,P_FLX_SCE_CFG_IND.END_PER_ELM_COD
        ,P_FLX_SCE_CFG_IND.ETI_ELM_COD                          AS ETI_GRP_COD
        ,P_FLX_SCE_CFG_IND.CUS_ELM_COD                          AS CUS_GRP_COD
        ,R_FLX_SCE.CUS_DIM_GRP_COD
        ,P_FLX_SCE_CFG_IND.PDT_ELM_COD                          AS PDT_GRP_COD
        ,P_FLX_SCE_CFG_IND.CAT_TYP_ELM_COD                      AS CAT_TYP_GRP_COD
        ,P_FLX_SCE_CFG_IND.EIB_ELM_COD                          AS EIB_GRP_COD
        ,P_FLX_SCE_CFG_IND.TTY_ELM_COD                          AS TTY_GRP_COD
        ,P_FLX_SCE_CFG_IND.SAL_SUP_ELM_COD                      AS SAL_SUP_GRP_COD
        ,F_CCD_FULL_DTL_PNL.IND_ELM_COD                         AS IND_ELM_COD
        ,R_FLX_GRP_IND.IND_NUM_FLG
        ,R_FLX_GRP_IND.IND_DEN_FLG
        ,F_CCD_FULL_DTL_PNL.PERIOD_ELEMENT_CODE                 AS PER_ELM_COD
        ,(CASE WHEN F_CCD_FULL_DTL_PNL.PERIOD_ELEMENT_CODE <= R_FLX_SCE.LST_ACT_PER_COD THEN 1 
               ELSE 0 
          END)                                                  AS ACT_PER_FLG
        ,F_CCD_FULL_DTL_PNL.ENTITY_ELEMENT_CODE                 AS ETI_ELM_COD
        ,F_CCD_FULL_DTL_PNL.CUSTOMER_ELEMENT_CODE               AS CUS_ELM_COD
        ,F_CCD_FULL_DTL_PNL.PRODUCT_ELEMENT_CODE                AS PDT_ELM_COD
        ,F_CCD_FULL_DTL_PNL.MGR_L500_CODE                       AS CAT_TYP_ELM_COD
        ,(CASE WHEN R_FLX_SCE.EIB_USE_FLG = 0 THEN 'NA'
               ELSE F_CCD_FULL_DTL_PNL.EIB_ELEMENT_CODE
          END)                                                  AS EIB_ELM_COD
        ,(CASE WHEN R_FLX_SCE.TTY_USE_FLG = 0 THEN 'NA'
               ELSE F_CCD_FULL_DTL_PNL.TERRITORY_ELEMENT_CODE
          END)                                                  AS TTY_ELM_COD
--        ,F_CCD_FULL_DTL_PNL.EIB_ELEMENT_CODE                    AS EIB_ELM_COD
--        ,F_CCD_FULL_DTL_PNL.TERRITORY_ELEMENT_CODE              AS TTY_ELM_COD
        ,F_CCD_FULL_DTL_PNL.SU_SP_SPLIT_CODE                    AS SAL_SUP_ELM_COD
        ,F_CCD_FULL_DTL_PNL.CUR_COD                             AS SRC_CUR_COD
        ,R_FLX_SCE.CUR_COD                                      AS SCE_CUR_COD
        ,(CASE R_FLX_SCE.CUR_COD
               WHEN 'EUR_CY' THEN F_CCD_FULL_DTL_PNL.RATE_CY
               WHEN 'EUR_FY' THEN F_CCD_FULL_DTL_PNL.RATE_FY
               WHEN 'LC' THEN 1
          END)                                                  AS CNV_CUR_RAT
    
        ,(CASE R_FLX_SCE.CUR_COD
               WHEN 'EUR_CY' THEN F_CCD_FULL_DTL_PNL.AMOUNT_EUR_CY
               WHEN 'EUR_FY' THEN F_CCD_FULL_DTL_PNL.AMOUNT_EUR_FY
               WHEN 'LC' THEN F_CCD_FULL_DTL_PNL.AMOUNT_LC
          END)                                                  AS AMOUNT
        
        ,F_CCD_FULL_DTL_PNL.ACCOUNT_ELEMENT_CODE
        ,F_CCD_FULL_DTL_PNL.DESTINATION_ELEMENT_CODE
        ,F_CCD_FULL_DTL_PNL.FUNCTIONAL_AREA_ELEMENT_CODE
        ,F_CCD_FULL_DTL_PNL.CATEGORY_ELEMENT_CODE
        ,F_CCD_FULL_DTL_PNL.CHANNEL_ELEMENT_CODE
        ,F_CCD_FULL_DTL_PNL.IOM_CODE
        ,F_CCD_FULL_DTL_PNL.PLANT_CODE
        ,F_CCD_FULL_DTL_PNL.T_UPD_TST
FROM     COP_DMT_FLX.R_FLX_SCE
         UNPIVOT (SRC_SCE_ELM_COD FOR COLNAME IN (CMP_1ST_SRC_SCE_COD
                                                 ,CMP_2ND_SRC_SCE_COD
                                                 ,CMP_3RD_SRC_SCE_COD))
         INNER JOIN COP_DMT_FLX.P_FLX_SCE_CFG_IND ON 
         (
           R_FLX_SCE.SCE_ELM_KEY = P_FLX_SCE_CFG_IND.SCE_ELM_KEY
         )
         INNER JOIN COP_DSP_CONTROLLING_CLOUD.F_CCD_FULL_DTL_PNL ON 
         (
           F_CCD_FULL_DTL_PNL.CBU_COD                     = R_FLX_SCE.CBU_COD                 AND
           F_CCD_FULL_DTL_PNL.SCENARIO_ELEMENT_CODE       = R_FLX_SCE.SRC_SCE_ELM_COD         AND
           F_CCD_FULL_DTL_PNL.PERIOD_ELEMENT_CODE   BETWEEN P_FLX_SCE_CFG_IND.BGN_PER_ELM_COD AND P_FLX_SCE_CFG_IND.END_PER_ELM_COD
         )
         -- To retrieve the volume unit configured for the Entity
         INNER JOIN COP_DMT_FLX.R_FLX_ETI ON 
         (
            R_FLX_ETI.CBU_COD     = F_CCD_FULL_DTL_PNL.CBU_COD             AND
            R_FLX_ETI.ETI_ELM_COD = F_CCD_FULL_DTL_PNL.ENTITY_ELEMENT_CODE
         )
         INNER JOIN COP_DMT_FLX.R_FLX_GRP_IND ON 
         (
            R_FLX_GRP_IND.IND_GRP_COD = P_FLX_SCE_CFG_IND.IND_ELM_COD AND
            R_FLX_GRP_IND.IND_ELM_COD = F_CCD_FULL_DTL_PNL.IND_ELM_COD  AND
            -- Retrieve the Volume in the unit configured for the Entity
            (CASE WHEN F_CCD_FULL_DTL_PNL.IND_ELM_COD LIKE 'VOL%' THEN 
                       F_CCD_FULL_DTL_PNL.IND_ELM_COD = 'VOL_'|| R_FLX_ETI.VOL_UNT_COD 
                  ELSE TRUE 
             END)
         )
WHERE    1=1
AND      R_FLX_SCE.INI_STS_COD IN ('in_progress:2')  -- TODO: Change status

AND      (P_FLX_SCE_CFG_IND.ETI_ELM_COD = '$TOTAL_ETI' 
OR        EXISTS (SELECT 1
                  FROM   COP_DMT_FLX.R_FLX_GRP_ETI
                  WHERE  R_FLX_GRP_ETI.ETI_GRP_COD = P_FLX_SCE_CFG_IND.ETI_ELM_COD
                  AND    R_FLX_GRP_ETI.ETI_ELM_COD = F_CCD_FULL_DTL_PNL.ENTITY_ELEMENT_CODE
                 )
         )

AND      (P_FLX_SCE_CFG_IND.CUS_ELM_COD = '$TOTAL_CUS' 
OR        EXISTS (SELECT 1
                  FROM   COP_DMT_FLX.R_FLX_GRP_CUS
                  WHERE  R_FLX_GRP_CUS.CUS_GRP_COD = P_FLX_SCE_CFG_IND.CUS_ELM_COD
                  AND    R_FLX_GRP_CUS.CUS_ELM_COD = F_CCD_FULL_DTL_PNL.CUSTOMER_ELEMENT_CODE
                 )
         )

AND      (P_FLX_SCE_CFG_IND.PDT_ELM_COD = '$TOTAL_PDT' 
OR        EXISTS (SELECT 1
                  FROM   COP_DMT_FLX.R_FLX_GRP_PDT
                  WHERE  R_FLX_GRP_PDT.PDT_GRP_COD = P_FLX_SCE_CFG_IND.PDT_ELM_COD
                  AND    R_FLX_GRP_PDT.PDT_ELM_COD = F_CCD_FULL_DTL_PNL.PRODUCT_ELEMENT_CODE
                 )
         )

AND      (P_FLX_SCE_CFG_IND.CAT_TYP_ELM_COD = '$TOTAL_CAT_TYP' 
OR        EXISTS (SELECT 1
                  FROM COP_DMT_FLX.R_FLX_GRP_CAT_TYP
                  WHERE R_FLX_GRP_CAT_TYP.CAT_TYP_GRP_COD = P_FLX_SCE_CFG_IND.CAT_TYP_ELM_COD
                      AND R_FLX_GRP_CAT_TYP.CAT_TYP_ELM_COD = F_CCD_FULL_DTL_PNL.MGR_L500_CODE
                  )
         )

AND      (P_FLX_SCE_CFG_IND.EIB_ELM_COD = '$TOTAL_EIB' 
OR        EXISTS (SELECT 1
                  FROM   COP_DMT_FLX.R_FLX_GRP_EIB
                  WHERE  R_FLX_GRP_EIB.EIB_GRP_COD = P_FLX_SCE_CFG_IND.EIB_ELM_COD
                  AND    R_FLX_GRP_EIB.EIB_ELM_COD = F_CCD_FULL_DTL_PNL.EIB_ELEMENT_CODE
                 )
         )

AND      (P_FLX_SCE_CFG_IND.TTY_ELM_COD = '$TOTAL_TTY' 
OR        EXISTS (SELECT 1
                  FROM   COP_DMT_FLX.R_FLX_GRP_TTY
                  WHERE  R_FLX_GRP_TTY.TTY_GRP_COD = P_FLX_SCE_CFG_IND.TTY_ELM_COD
                  AND    R_FLX_GRP_TTY.TTY_ELM_COD = F_CCD_FULL_DTL_PNL.TERRITORY_ELEMENT_CODE
                 )
         )

AND      (P_FLX_SCE_CFG_IND.SAL_SUP_ELM_COD = '$TOTAL_SAL_SUP' 
OR        EXISTS (SELECT 1
                  FROM   COP_DMT_FLX.R_FLX_GRP_SAL_SUP
                  WHERE  R_FLX_GRP_SAL_SUP.SAL_SUP_GRP_COD = P_FLX_SCE_CFG_IND.SAL_SUP_ELM_COD
                  AND    R_FLX_GRP_SAL_SUP.SAL_SUP_ELM_COD = F_CCD_FULL_DTL_PNL.SU_SP_SPLIT_CODE
                 )
         )

-- Only keep the latest config for each record
QUALIFY  P_FLX_SCE_CFG_IND.CFG_ORD_NUM = MAX(P_FLX_SCE_CFG_IND.CFG_ORD_NUM) 
                                         OVER (PARTITION BY P_FLX_SCE_CFG_IND.CBU_COD
                                                           ,P_FLX_SCE_CFG_IND.SCE_ELM_COD
                                                           ,P_FLX_SCE_CFG_IND.SCE_ELM_KEY
                                                           ,R_FLX_SCE.SRC_SCE_ELM_COD
                                                           ,F_CCD_FULL_DTL_PNL.IND_ELM_COD
         -- For indicators, we want to keep only one version for the indicators that will be used in the target scenario
         -- but we want to keep all the versions for denominators used to calculate ratios (they will be discarded after calculation).
--                                                           ,(CASE WHEN IND_NUM_FLG = 0 THEN IND_GRP_COD 
--                                                                  ELSE 'ONLY_LATEST' 
--                                                             END)
                                                           ,F_CCD_FULL_DTL_PNL.PERIOD_ELEMENT_CODE
                                                           ,F_CCD_FULL_DTL_PNL.ENTITY_ELEMENT_CODE
                                                           ,F_CCD_FULL_DTL_PNL.CUSTOMER_ELEMENT_CODE
                                                           ,F_CCD_FULL_DTL_PNL.PRODUCT_ELEMENT_CODE
                                                           ,F_CCD_FULL_DTL_PNL.MGR_L500_CODE
                                                           ,F_CCD_FULL_DTL_PNL.EIB_ELEMENT_CODE
                                                           ,F_CCD_FULL_DTL_PNL.TERRITORY_ELEMENT_CODE
                                                           ,F_CCD_FULL_DTL_PNL.SU_SP_SPLIT_CODE
                                                           )
)
SELECT   CBU_COD
        ,SCE_ELM_COD
        ,ANY_VALUE(CFG_ORD_NUM)     AS CFG_ORD_NUM
        ,SRC_SCE_ELM_COD
        ,IND_GRP_COD
        ,ANY_VALUE(IND_GRP_RAT_FLG) AS IND_GRP_RAT_FLG
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
        ,ANY_VALUE(IND_NUM_FLG) AS IND_NUM_FLG
        ,ANY_VALUE(IND_DEN_FLG) AS IND_DEN_FLG
        ,PER_ELM_COD
        ,ANY_VALUE(ACT_PER_FLG) AS ACT_PER_FLG
        ,ETI_ELM_COD
        ,CUS_ELM_COD
        ,PDT_ELM_COD
        ,CAT_TYP_ELM_COD
        ,EIB_ELM_COD
        ,TTY_ELM_COD
        ,SAL_SUP_ELM_COD
        ,SRC_CUR_COD
        ,ANY_VALUE(SCE_CUR_COD) AS SCE_CUR_COD
        ,ANY_VALUE(CNV_CUR_RAT) AS CNV_CUR_RAT
        ,1                      AS RTO_DEN_AMT
        ,1                      AS BAS_DEN_AMT
        ,SUM(AMOUNT) AS AMOUNT
        ,ACCOUNT_ELEMENT_CODE
        ,DESTINATION_ELEMENT_CODE
        ,FUNCTIONAL_AREA_ELEMENT_CODE
        -- These dimensions are not useful for the disaggregation
        ,ANY_VALUE(CATEGORY_ELEMENT_CODE) AS CATEGORY_ELEMENT_CODE
        ,ANY_VALUE(CHANNEL_ELEMENT_CODE) AS CHANNEL_ELEMENT_CODE
        ,ANY_VALUE(IOM_CODE) AS IOM_CODE
        ,ANY_VALUE(PLANT_CODE) AS PLANT_CODE
        ,MAX(T_UPD_TST) AS T_REC_SRC_TST
        ,CURRENT_TIMESTAMP AS T_REC_INS_TST
        ,CURRENT_TIMESTAMP AS T_REC_UPD_TST
FROM     FINAL
GROUP BY ALL;

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

        -- xx. Update all scenarios in_progress:2 to failed in case of failure
        v_STEP_TABLE := 'UPDATE STATUS TO failed IN R_FLX_SCE';
        v_STEP_NUM   := v_STEP_NUM + 1;
        
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_BEG_DT;

        UPDATE COP_DMT_FLX.R_FLX_SCE
        SET    INI_STS_COD = 'failed'
        WHERE  INI_STS_COD = 'in_progress:2';

        -- Assign the end date of the step
        SELECT TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DDTHH24:MI:SS.FF3Z') INTO :V_STEP_END_DT;

        -- Call the procedure to log the step
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_STP_LOG('SNOW', :V_RUN_ID, :v_STEP_TABLE, :v_STEP_NUM, 0, NULL, :V_STEP_BEG_DT, :V_STEP_END_DT, NULL, 0, 0, 0, 0, 0, 0, 0, 0);

        -- Call the procedure to log the step
        CALL COP_DMT_FLX.SP_T_FLX_FLW_RUN_LOG_END('SNOW', :V_RUN_ID);
        
        RETURN SQLCODE || ': ' || SQLERRM;
        RAISE; -- Raise the same exception that you are handling.

END;

$$
;