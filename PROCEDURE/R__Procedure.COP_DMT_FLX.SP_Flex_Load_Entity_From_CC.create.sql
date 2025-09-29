USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Entity_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_ETI
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 27-06-2024                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);
BEGIN

    -- Load the table R_FLX_ETI in frp mode

    v_STEP_TABLE := 'R_FLX_ETI';

    UPDATE COP_DMT_FLX.R_FLX_ETI FLEX SET T_REC_DLT_FLG = 1;

    MERGE INTO COP_DMT_FLX.R_FLX_ETI FLEX USING (SELECT CBU_COD || '-' || ENTITY_CODE           ETI_ELM_KEY
                                                       ,ENTITY_CODE                             ETI_ELM_COD
                                                       ,ENTITY_DESC                             ETI_ELM_DSC
                                                       ,CBU_COD                                 CBU_COD
                                                       ,FIN_REPORTING_UNIT_CODE                 ETI_CRY_COD
                                                       ,FIN_REPORTING_UNIT_DESC                 ETI_CRY_DSC
                                                       ,(CASE WHEN CBU_COD = 'IBE' THEN 'EUR'
                                                              WHEN CBU_COD = 'DCH' THEN
                                                                   (CASE ENTITY_CODE
                                                                         WHEN 'CH01' THEN 'CHF'
                                                                         ELSE 'EUR'
                                                                    END)
                                                              WHEN CBU_COD = 'POL' THEN 'PLN'
                                                              ELSE 'EUR'
                                                         END)                                   ETI_CUR_COD
                                                       ,0                                       T_REC_DLT_FLG
                                                       ,CURRENT_TIMESTAMP                       T_REC_INS_TST
                                                       ,CURRENT_TIMESTAMP                       T_REC_UPD_TST
                                                 FROM   PRD_COP.COP_DSP_CONTROLLING_CLOUD.R_ENTITY_UNIFY) CCD
    ON FLEX.ETI_ELM_KEY = CCD.ETI_ELM_KEY
    WHEN MATCHED THEN 
         UPDATE SET FLEX.ETI_ELM_COD   = CCD.ETI_ELM_COD
                   ,FLEX.ETI_ELM_DSC   = CCD.ETI_ELM_DSC
                   ,FLEX.CBU_COD       = CCD.CBU_COD
                   ,FLEX.ETI_CRY_COD   = CCD.ETI_CRY_COD
                   ,FLEX.ETI_CRY_DSC   = CCD.ETI_CRY_DSC
                   ,FLEX.T_REC_DLT_FLG = CCD.T_REC_DLT_FLG
                   ,FLEX.T_REC_UPD_TST = CCD.T_REC_UPD_TST
    WHEN NOT MATCHED THEN 
         INSERT (ETI_ELM_KEY
                ,ETI_ELM_COD
                ,ETI_ELM_DSC
                ,CBU_COD
                ,ETI_CRY_COD
                ,ETI_CRY_DSC
                ,T_REC_DLT_FLG
                ,T_REC_INS_TST
                ,T_REC_UPD_TST
                )
         VALUES (CCD.ETI_ELM_KEY
                ,CCD.ETI_ELM_COD
                ,CCD.ETI_ELM_DSC
                ,CCD.CBU_COD
                ,CCD.ETI_CRY_COD
                ,CCD.ETI_CRY_DSC
                ,CCD.T_REC_DLT_FLG
                ,CCD.T_REC_INS_TST
                ,CCD.T_REC_UPD_TST
                )
    ;

    v_STEP_TABLE := 'R_FLX_GRP_ETI';

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_GRP_ETI;

    INSERT OVERWRITE ALL
    INTO COP_DMT_FLX.R_FLX_GRP_ETI
         (CBU_COD, ETI_ELM_COD, ETI_DIM_GRP_COD, ETI_GRP_COD, ETI_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
         VALUES (CBU_COD, ETI_ELM_COD, -1, '$TOTAL_ETI', 'Total Entity', T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
    INTO COP_DMT_FLX.R_FLX_GRP_ETI
         (CBU_COD, ETI_ELM_COD, ETI_DIM_GRP_COD, ETI_GRP_COD, ETI_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
         VALUES (CBU_COD, ETI_ELM_COD,  0, ETI_ELM_COD, eti_elm_dsc, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
    INTO COP_DMT_FLX.R_FLX_GRP_ETI
         (CBU_COD, ETI_ELM_COD, ETI_DIM_GRP_COD, ETI_GRP_COD, ETI_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
         VALUES (CBU_COD, ETI_ELM_COD,  1, ETI_CRY_COD, ETI_CRY_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
    INTO COP_DMT_FLX.R_FLX_GRP_ETI
         (CBU_COD, ETI_ELM_COD, ETI_DIM_GRP_COD, ETI_GRP_COD, ETI_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
         VALUES (CBU_COD, ETI_ELM_COD,  2, CBU_COD, CBU_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
    SELECT   eti.CBU_COD
            ,cbu.CBU_DSC
            ,eti.ETI_ELM_COD
            ,eti.ETI_ELM_DSC
            ,REPLACE(REPLACE(eti.ETI_CRY_COD
                            ,'ETI_FNC_BU_', '')
                    ,'GEO_CRY_L3-', '')                  as ETI_CRY_COD
            ,eti.ETI_CRY_DSC
            ,0                                           as T_REC_DLT_FLG
            ,current_timestamp                           as T_REC_INS_TST
            ,current_timestamp                           as T_REC_UPD_TST
    FROM     COP_DMT_FLX.R_FLX_ETI as eti
             INNER JOIN R_FLX_CBU as cbu ON
             (
                 eti.CBU_COD = cbu.CBU_COD
             )
    WHERE    eti.T_REC_DLT_FLG = 0
    ORDER BY CBU_COD, ETI_ELM_COD;

    -- Return text when the stored procedure completes

    RETURN 'Success';

EXCEPTION
    WHEN OTHER THEN
         RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
         RAISE; -- Raise the same exception that you are handling.
END;
$$
;
