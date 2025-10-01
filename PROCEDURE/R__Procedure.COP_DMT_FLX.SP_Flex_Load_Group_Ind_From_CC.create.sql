USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Group_Ind_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_GRP_IND
Author      : Mohammedi Yanis (Solution BI France)                      
Created On  : 28-06-2024                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);
BEGIN

   -- Load the table R_FLX_ETI in frp mode
   
   v_STEP_TABLE := 'Truncate R_FLX_GRP_IND';

   TRUNCATE TABLE COP_DMT_FLX.R_FLX_GRP_IND;

   v_STEP_TABLE := 'Insert into R_FLX_GRP_IND FROM V_ETL_R_FLX_GRP_IND';

   INSERT INTO R_FLX_GRP_IND
   SELECT * FROM V_ETL_R_FLX_GRP_IND;

   RETURN 'Success';

EXCEPTION
   WHEN OTHER THEN
        RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
        RAISE; -- Raise the same exception that you are handling.
END
$$;
;
