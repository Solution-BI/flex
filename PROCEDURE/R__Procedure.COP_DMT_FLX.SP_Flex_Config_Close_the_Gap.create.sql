USE SCHEMA COP_DMT_FLX;
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Populating T_FLX_SEC_USR Table

Author      : Yanis MOHAMMMEDI (Solution BI France)
Created On  : 28-11-2024
=========================================================================
Modified On:    Description:                        Author:
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_FLEX_CONFIG_CLOSE_THE_GAP(JSON_INPUT VARCHAR(16777216))
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

	// This method enables the standard call to a sequential array of queries
	
	function uuidHelper() {
		return ( ( ( 1+Math.random() ) * 0x10000 ) | 0 ).toString( 16 ).substring( 1 );
	}
	
	function generateuuid() {
		return (uuidHelper() + uuidHelper() + "-" + uuidHelper() + "-4" 
			+ uuidHelper().substr(0,2) + "-" + uuidHelper() + "-" + uuidHelper() + uuidHelper() + uuidHelper()
		).toLowerCase();
	}

   var RUN_ID = generateuuid();

   /* Logging incoming JSON data */
   var user_cmd = `SELECT DISTINCT
                          REPLACE(T_REC_USER,'"')                                                 AS T_REC_EML_USR
                   FROM   (SELECT T_REC_USER
                           FROM   (SELECT THIS:User AS T_REC_USER
                                   FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}'))));
                  `;
   var user_res = snowflake.execute( {sqlText: user_cmd} );
   user_res.next();
   var v_eml_user = user_res.getColumnValue(1);

   var log_data = snowflake.createStatement({sqlText: `CALL SP_Flex_Log_PowerOn(:1,'SP_Flex_Config_Close_The_Gap','${JSON_INPUT}',:2);`, binds:[RUN_ID,v_eml_user]});
   log_data.execute();
   
   /* Insert/Update/Delete Check */
   var cmd_crud_check = `SELECT MAX(CASE WHEN PATH = 'InsertedRows' THEN LENGTH(VALUE)
                                         ELSE 0
                                    END) InsertedRows
                              ,MAX(CASE WHEN PATH = 'UpdatedRows' THEN LENGTH(VALUE)
                                        ELSE 0
                                   END) UpdatedRows
                              ,MAX(CASE WHEN PATH = 'DeletedRows' THEN LENGTH(VALUE)
                                       ELSE 0
                                   END) DeletedRows
                              ,MAX(CASE WHEN PATH = 'User' THEN VALUE
                                        ELSE ''
                                   END) userName
                              ,to_varchar(CURRENT_TIMESTAMP, 'yyyy-mm-dd hh:mi:ss.ff')
                         FROM LATERAL FLATTEN(parse_json('${JSON_INPUT}'));`;
   
   var crud_res = snowflake.execute( {sqlText: cmd_crud_check} );
   crud_res.next();
   var isInserted = crud_res.getColumnValue(1) > 2;
   var isUpdated = crud_res.getColumnValue(2) > 2;
   var isDeleted = crud_res.getColumnValue(3) > 2;
   
   /* Update Operation Block */
   if (isUpdated) {
      /* Fetching data from JSON INPUT */
      var fch_data = `SELECT DISTINCT
                             REPLACE(X.upd_row:ID,'"')::NUMBER                                   AS ID
                            ,REPLACE(X.upd_row:ETI_ELM_COD,'"')                                  AS ETI_ELM_COD
                            ,REPLACE(X.upd_row:LV0_PDT_CAT_COD,'"')                              AS LV0_PDT_CAT_COD
                            ,COALESCE(REPLACE(x.upd_row:GAP_CLO_PER_COD,'"'),'12')               AS GAP_CLO_PER_COD
                            ,SUBSTR(REPLACE(X.USER,'"')
                                   ,1
                                   ,POSITION('@',REPLACE(X.USER,'"'),1) - 1
                                   )                                                             AS T_REC_USR
                            ,REPLACE(X.USER,'"')                                                 AS T_REC_EML_USR
                      FROM   (SELECT parse_json(b.VALUE:Updated) as upd_row
                                    ,a.USER
                              FROM   (SELECT VALUE
                                            ,THIS:User AS USER
                                      FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                                      WHERE  PATH LIKE '%UpdatedRows%') a
                                     ,LATERAL FLATTEN(a.VALUE) b) X;
                    `;
   
      var stmt = snowflake.createStatement( {sqlText: fch_data} );
      var res = stmt.execute();
   
      var v_id;
      var v_eti_elm_cod;
      var v_lv0_pdt_car_cod;
      var v_gap_clo_per_cod;
      var v_user;
      var v_eml_user;

      var upd_cmd;
      var upd_stmt;

      while (res.next()) {

         /* Assigning data into local variables */
         v_id = res.getColumnValue(1);
         v_eti_elm_cod = res.getColumnValue(2);
         v_lv0_pdt_car_cod = res.getColumnValue(3);
         v_gap_clo_per_cod = res.getColumnValue(4);
         v_user = res.getColumnValue(5);
         v_eml_user = res.getColumnValue(6);

         upd_cmd = `UPDATE COP_DMT_FLX.P_FLX_GAP_CLO_CFG
                    SET    GAP_CLO_PER_COD = '${v_gap_clo_per_cod}'
                          ,T_REC_UPD_TST   = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                    WHERE  ID  = '${v_id}';
                   `;
         upd_stmt = snowflake.createStatement( {sqlText: upd_cmd} );
         upd_stmt.execute();

      }

   }
/* End of Update Operation Block */

$$
;
