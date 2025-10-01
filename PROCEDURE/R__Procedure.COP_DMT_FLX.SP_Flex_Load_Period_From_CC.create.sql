USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Period_From_CC() 
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_PER
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 01-07-2024                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);
BEGIN

   -- Load the table R_FLX_PDT in frp mode

   v_STEP_TABLE := 'R_FLX_PER';

   TRUNCATE TABLE COP_DMT_FLX.R_FLX_PER;

   INSERT INTO COP_DMT_FLX.R_FLX_PER
         (PER_ELM_COD
         ,PER_ELM_DSC
         ,QOY_ELM_COD
         ,QOY_ELM_DSC
         ,SOY_ELM_COD
         ,SOY_ELM_DSC
         ,FYR_ELM_COD
         ,FYR_ELM_DSC
         ,T_REC_DLT_FLG
         ,T_REC_INS_TST
         ,T_REC_UPD_TST
         )
   SELECT DISTINCT 
          PER_ELM_IDT                                                                    PER_ELM_COD
         ,PER_ELM_DSC                                                                    PER_ELM_DSC
         ,(CASE WHEN PER_ELM_IDT IN ('01','02','03') THEN 'Q1'
                WHEN PER_ELM_IDT IN ('04','05','06') THEN 'Q2'
                WHEN PER_ELM_IDT IN ('07','08','09') THEN 'Q3'
                WHEN PER_ELM_IDT IN ('10','11','12') THEN 'Q4'
           END)                                                                          QOY_ELM_COD
         ,(CASE WHEN PER_ELM_IDT IN ('01','02','03') THEN '1st Quarter'
                WHEN PER_ELM_IDT IN ('04','05','06') THEN '2nd Quarter'
                WHEN PER_ELM_IDT IN ('07','08','09') THEN '3rd Quarter'
                WHEN PER_ELM_IDT IN ('10','11','12') THEN '4th Quarter'
           END)                                                                          QOY_ELM_DSC
         ,(CASE WHEN PER_ELM_IDT IN ('01','02','03','04','05','06') THEN 'S1'
                WHEN PER_ELM_IDT IN ('07','08','09','10','11','12') THEN 'S2'
           END)                                                                          SOY_ELM_COD
         ,(CASE WHEN PER_ELM_IDT IN ('01','02','03','04','05','06') THEN '1st Semester'
                WHEN PER_ELM_IDT IN ('07','08','09','10','11','12') THEN '2nd Semester'
           END)                                                                          SOY_ELM_DSC
         ,'FY'                                                                           FYR_ELM_COD
         ,'Full year'                                                                    FYR_ELM_DSC
         ,0                                                                              T_REC_DLT_FLG
         ,CURRENT_TIMESTAMP                                                              T_REC_INS_TST
         ,CURRENT_TIMESTAMP                                                              T_REC_UPD_TST
   FROM   PRD_COP.COP_DMT_CCD.R_CCD_PER
   WHERE  CBU_COD='CEN';

  -- Return text when the stored procedure completes

   v_STEP_TABLE := 'R_FLX_GRP_PER';

   TRUNCATE TABLE COP_DMT_FLX.R_FLX_GRP_PER;

   INSERT OVERWRITE ALL
       INTO COP_DMT_FLX.R_FLX_GRP_PER
           (CBU_COD, PER_ELM_COD, PER_DIM_GRP_COD, PER_GRP_COD, PER_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
           VALUES (CBU_COD, PER_ELM_COD, -1, '$TOTAL_PER', 'Total Period', T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PER
           (CBU_COD, PER_ELM_COD, PER_DIM_GRP_COD, PER_GRP_COD, PER_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
           VALUES (CBU_COD, PER_ELM_COD,  0, PER_ELM_COD, PER_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PER
           (CBU_COD, PER_ELM_COD, PER_DIM_GRP_COD, PER_GRP_COD, PER_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
           VALUES (CBU_COD, PER_ELM_COD,  1, QOY_ELM_COD, QOY_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PER
           (CBU_COD, PER_ELM_COD, PER_DIM_GRP_COD, PER_GRP_COD, PER_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
           VALUES (CBU_COD, PER_ELM_COD,  2, SOY_ELM_COD, SOY_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PER
           (CBU_COD, PER_ELM_COD, PER_DIM_GRP_COD, PER_GRP_COD, PER_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
           VALUES (CBU_COD, PER_ELM_COD,  3, FYR_ELM_COD, FYR_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
   SELECT   cbu.CBU_COD
           ,per.PER_ELM_COD
           ,per.PER_ELM_DSC
           ,per.QOY_ELM_COD
           ,per.QOY_ELM_DSC
           ,per.SOY_ELM_COD
           ,per.SOY_ELM_DSC
           ,per.FYR_ELM_COD
           ,per.FYR_ELM_DSC
           ,0                 as T_REC_DLT_FLG
           ,CURRENT_TIMESTAMP as T_REC_INS_TST
           ,CURRENT_TIMESTAMP as T_REC_UPD_TST
   from     COP_DMT_FLX.R_FLX_PER AS per
            CROSS JOIN COP_DMT_FLX.R_FLX_CBU AS cbu
   ;

  RETURN 'Success';

EXCEPTION
   WHEN OTHER THEN
        RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
        RAISE; -- Raise the same exception that you are handling.
END;
$$
;
