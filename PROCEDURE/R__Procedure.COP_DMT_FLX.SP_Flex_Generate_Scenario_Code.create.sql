USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE PROCEDURE SP_Flex_Generate_Scenario_Code(P_SCE_YEA_COD VARCHAR,P_SCE_PRO_COD VARCHAR,P_SCE_MKT_COD VARCHAR,P_DTA_YEA_COD VARCHAR)
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS CALLER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to generate the scenario code
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 27-06-2024                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

DECLARE
  v_sce_cod     VARCHAR(30);
  v_ver_num     VARCHAR(10);
  v_sce_elm_cod VARCHAR(30);
  c1 CURSOR FOR 
     SELECT TO_CHAR(TRUNC(NVL(MAX(TO_NUMBER(REPLACE(SCE_ELM_COD,?))),0) + 1 )) AS VER_NUM
     FROM   COP_DMT_FLX.R_FLX_SCE
     WHERE  SCE_ELM_COD LIKE ? || '%' AND
            CBU_COD = ?;

BEGIN
   -- Define the prefix of the scenario code
   v_sce_cod := P_SCE_YEA_COD || '_' ||
                P_SCE_PRO_COD || '_' ||
                (CASE WHEN P_DTA_YEA_COD < P_SCE_YEA_COD  THEN 'PY'
                              ELSE 'N' || 
                                   TO_CHAR(TRUNC(P_DTA_YEA_COD - P_SCE_YEA_COD,0))
                 END)         || 
                '_FLEX_V';
   -- Define the maximum of the version for the prefix
   OPEN c1 using (v_sce_cod,v_sce_cod,p_sce_mkt_cod);
   LOOP
      FETCH c1 into v_ver_num;
      IF (v_ver_num <> '') THEN
         v_sce_elm_cod := v_sce_cod || v_ver_num;
      ELSE
         BREAK;
      END IF;
   END LOOP;
   CLOSE c1;
   -- return the scenario code
   RETURN v_sce_elm_cod;
END;
$$
;