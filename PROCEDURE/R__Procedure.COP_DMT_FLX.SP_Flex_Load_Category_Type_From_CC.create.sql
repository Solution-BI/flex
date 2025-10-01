USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Category_Type_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_CAT_TYP
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

    v_STEP_TABLE := 'R_FLX_CAT_TYP';

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_CAT_TYP;

    INSERT INTO R_FLX_CAT_TYP
           (CAT_TYP_ELM_KEY
           ,CBU_COD
           ,CAT_TYP_ELM_COD
           ,CAT_TYP_ELM_DSC
           ,T_REC_DLT_FLG
           ,T_REC_INS_TST
           ,T_REC_UPD_TST
           )
    VALUES ('DCH-MGR' , 'DCH', 'MGR' , 'Managerial'                ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ,('DCH-L500', 'DCH', 'L500', 'Internal Transfers'        ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ,('DCH-NA'  , 'DCH', 'NA'  , 'N/A'                       ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ,('IBE-MGR' , 'IBE', 'MGR' , 'Managerial'                ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ,('IBE-L500', 'IBE', 'L500', 'Internal Transfers'        ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ,('IBE-NA'  , 'IBE', 'NA'  , 'N/A'                       ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ,('POL-MGR' , 'POL', 'MGR' , 'Managerial'                ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ,('POL-L500', 'POL', 'L500', 'Internal Transfers'        ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ,('POL-NA'  , 'POL', 'NA'  , 'N/A'                       ,  0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ;

    v_STEP_TABLE := 'R_FLX_GRP_CAT_TYP';

    TRUNCATE TABLE COP_DMT_FLX.R_FLX_GRP_CAT_TYP;

    INSERT OVERWRITE ALL
    INTO COP_DMT_FLX.R_FLX_GRP_CAT_TYP
         (CBU_COD, CAT_TYP_ELM_COD, CAT_TYP_DIM_GRP_COD, CAT_TYP_GRP_COD, CAT_TYP_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
         VALUES (CBU_COD, CAT_TYP_ELM_COD, -1, '$TOTAL_CAT_TYP', 'IFRS', T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
    INTO COP_DMT_FLX.R_FLX_GRP_CAT_TYP
         (CBU_COD, CAT_TYP_ELM_COD, CAT_TYP_DIM_GRP_COD, CAT_TYP_GRP_COD, CAT_TYP_GRP_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
         VALUES (CBU_COD, CAT_TYP_ELM_COD,  0, CAT_TYP_ELM_COD, CAT_TYP_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
    SELECT   CBU_COD
            ,CAT_TYP_ELM_COD
            ,CAT_TYP_ELM_DSC
            ,0                                           as T_REC_DLT_FLG
            ,current_timestamp                           as T_REC_INS_TST
            ,current_timestamp                           as T_REC_UPD_TST
    FROM     COP_DMT_FLX.R_FLX_CAT_TYP
    ORDER BY CBU_COD, CAT_TYP_ELM_COD;

    RETURN 'Success';

EXCEPTION
    WHEN OTHER THEN
         RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
         RAISE; -- Raise the same exception that you are handling.
END
$$;
;
