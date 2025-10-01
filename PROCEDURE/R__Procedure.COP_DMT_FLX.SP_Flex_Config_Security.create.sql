USE SCHEMA COP_DMT_FLX;
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Populating T_FLX_SEC_USR Table

Author      : Noel Coquio (Solution BI France)
Created On  : 05-11-2024
=========================================================================
Modified On:    Description:                        Author:
=========================================================================
*/
CREATE OR REPLACE PROCEDURE SP_Flex_Config_Security(JSON_INPUT VARCHAR(16777216))
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

var log_data = snowflake.createStatement({sqlText: `CALL SP_Flex_Log_PowerOn(:1,'SP_Flex_Config_Security','${JSON_INPUT}',:2);`, binds:[RUN_ID,v_eml_user]});
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

/* Insert Operation Block */
if (isInserted) {
   /* Fetching data from JSON INPUT */
   var fch_data = `
SELECT DISTINCT
       REPLACE(X.ins_row:ID,'"')::NUMBER                                   AS ID
      ,REPLACE(X.ins_row:EML_USR_COD,'"')                                  AS EML_USR_COD
      ,REPLACE(X.ins_row:USR_CAN_DEL_SCE_FLG,'"')                          AS USR_CAN_DEL_SCE_FLG
      ,SUBSTR(REPLACE(X.USER,'"')
             ,1
             ,POSITION('@',REPLACE(X.USER,'"'),1) - 1
             )                                                             AS T_REC_USR
      ,REPLACE(X.USER,'"')                                                 AS T_REC_EML_USR
FROM   (SELECT parse_json(b.VALUE) as ins_row
              ,a.USER
        FROM   (SELECT VALUE
                      ,THIS:User AS USER
                FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                WHERE  PATH LIKE '%InsertedRows%') a
               ,LATERAL FLATTEN(a.VALUE) b) X;
                    `;

   var stmt = snowflake.createStatement( {sqlText: fch_data} );
   var res = stmt.execute();

   var v_id;
   var v_eml_usr_cod;
   var v_usr_can_del_sce_flg;
   var v_user;
   var v_user_eml;

   /* Check duplicate in T_FLX_SEC_USR */
   var dup_check_cmd;
   var dup_check_stmt;
   var dup_check_res;
   var v_is_dup = 0;
   var ins_data_cmd;
   var dup_message;

   while (res.next())  {

      /* Assigning data into local variables */
      v_id = res.getColumnValue(1);
      v_eml_usr_cod = res.getColumnValue(2);
      v_usr_can_del_sce_flg = res.getColumnValue(3);
      v_user = res.getColumnValue(4);
      v_user_eml = res.getColumnValue(5);

      /* Check duplicate in T_FLX_SEC_USR */
      dup_check_cmd = `SELECT COUNT(*) AS RowCount 
                       FROM   COP_DMT_FLX.T_FLX_SEC_USR 
                       WHERE  EML_USR_COD = '${v_eml_usr_cod}';`;
      dup_check_stmt = snowflake.createStatement( {sqlText: dup_check_cmd} );
      dup_check_res = dup_check_stmt.execute();
      dup_check_res.next();

      v_is_dup = dup_check_res.getColumnValue(1) > 0;

      /* If not a duplicate, Insert */
      if (!v_is_dup) {
         /* Inserting User to T_FLX_SEC_USR */

         ins_data_cmd = `INSERT INTO COP_DMT_FLX.T_FLX_SEC_USR
                                    (EML_USR_COD
                                    ,ROL_COD
                                    ,CRE_EML_USR_COD
                                    ,UPD_EML_USR_COD
                                    ,T_REC_DLT_FLG
                                    ,T_REC_INS_TST
                                    ,T_REC_UPD_TST
                                    )
                            SELECT   DISTINCT
                                     '${v_eml_usr_cod}'                  AS EML_USR_COD
                                    ,DECODE(COALESCE(${v_usr_can_del_sce_flg},'0')
                                           ,'0','<DEFAULT>'
                                           ,'CAN_DEL_SCENARIO'
                                           )                             AS ROL_COD
                                    ,'${v_user_eml}'                     AS CRE_EML_USR_COD
                                    ,'${v_user_eml}'                     AS UPD_EML_USR_COD
                                    ,0                                   AS T_REC_DLT_FLG
                                    ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS T_REC_INS_TST
                                    ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS T_REC_UPD_TST;
                           `;
         snowflake.execute( {sqlText: ins_data_cmd} );

      }
      else {
         dup_message = "<SQLError>A row already exists for the email " + v_eml_usr_cod + ".</SQLError>";
         throw dup_message;
      }

   }

}
/* End of Insert Operation Block */

/* Update Operation Block */
if (isUpdated) {

   var fch_data = `
SELECT DISTINCT
       REPLACE(X.upd_row:ID,'"')::NUMBER                                   AS ID
      ,REPLACE(X.upd_row:EML_USR_COD,'"')                                  AS EML_USR_COD
      ,REPLACE(X.upd_row:USR_CAN_DEL_SCE_FLG,'"')                          AS USR_CAN_DEL_SCE_FLG
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
   var v_eml_usr_cod;
   var v_usr_can_del_sce_flg;
   var v_user;
   var v_user_eml;

   /* Check duplicate in T_FLX_SEC_USR */
   var dup_check_cmd;
   var dup_check_stmt;
   var dup_check_res;
   var v_is_dup = 0;
   var upd_data_cmd;
   var dup_message;

   while (res.next())  {

      /* Assigning data into local variables */
      v_id = res.getColumnValue(1);
      v_eml_usr_cod = res.getColumnValue(2);
      v_usr_can_del_sce_flg = res.getColumnValue(3);
      v_user = res.getColumnValue(4);
      v_user_eml = res.getColumnValue(5);

      /* Check duplicate in T_FLX_SEC_USR */
      dup_check_cmd = `SELECT COUNT(*) AS RowCount 
                       FROM   COP_DMT_FLX.T_FLX_SEC_USR 
                       WHERE  EML_USR_COD  = '${v_eml_usr_cod}'
                       AND    ID          != '${v_id}';`;
      dup_check_stmt = snowflake.createStatement( {sqlText: dup_check_cmd} );
      dup_check_res = dup_check_stmt.execute();
      dup_check_res.next();

      v_is_dup = dup_check_res.getColumnValue(1) > 0;

      /* If not a duplicate, Insert */
      if (!v_is_dup) {

        upd_data_cmd = `UPDATE COP_DMT_FLX.T_FLX_SEC_USR
                        SET    EML_USR_COD     = '${v_eml_usr_cod}'
                              ,ROL_COD         = DECODE(COALESCE(${v_usr_can_del_sce_flg},'0')
                                                       ,'0','<DEFAULT>'
                                                       ,'CAN_DEL_SCENARIO'
                                                       )
                              ,UPD_EML_USR_COD = '${v_user_eml}'
                              ,T_REC_UPD_TST   = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                        WHERE  ID  = '${v_id}';
                       `;
        stmt = snowflake.createStatement( {sqlText: upd_data_cmd} );
        stmt.execute();

      }
      else {
         dup_message = "<SQLError>A row already exists for the email " + v_eml_usr_cod + ".</SQLError>";
         throw dup_message;
      }
   }

}
/* End of Update Operation Block */

$$
;
