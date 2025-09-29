USE SCHEMA COP_DMT_FLX{{uid}};

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Populating F_FLX_MAN_SCE Table

Author      : Noel Coquio (Solution BI France)
Created On  : 28-02-2025
=========================================================================
Modified On:    Description:                        Author:
=========================================================================
*/

CREATE OR REPLACE PROCEDURE SP_Flex_Insert_Manual_Data(JSON_INPUT VARCHAR(16777216))
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

	// This method enables the standard call to a sequential array of queries
	
	function uuidHelper() {
		return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
	}
	
	function generateuuid() {
		return (uuidHelper() + uuidHelper() + "-" + uuidHelper() + "-4" 
			+ uuidHelper().substring(0,2) + "-" + uuidHelper() + "-" + uuidHelper() + uuidHelper() + uuidHelper()
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

var log_data = snowflake.createStatement({sqlText: `CALL SP_Flex_Log_PowerOn(:1,'SP_Flex_Insert_Manual_Data','${JSON_INPUT}',:2);`, binds:[RUN_ID,v_eml_user]});
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
    /* Inserting data from JSON INPUT */

    var sts_check_cmd = `
SELECT   COUNT(*),LISTAGG(json.MAN_SCE_ELM_KEY,', ') list_
FROM     (SELECT   DISTINCT
                   REPLACE(X.ins_row:MAN_SCE_ELM_KEY,'"')                               AS MAN_SCE_ELM_KEY
          FROM     (SELECT parse_json(b.VALUE) as ins_row
                    FROM   (SELECT VALUE
                            FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                            WHERE  PATH LIKE '%InsertedRows%') a
                           ,LATERAL FLATTEN(a.VALUE) b) X) json
         LEFT OUTER JOIN R_FLX_MAN_SCE ON
         (
             R_FLX_MAN_SCE.MAN_SCE_ELM_KEY  = json.MAN_SCE_ELM_KEY
         )
WHERE    MAN_SCE_USE_FLG IS NULL;
                        `;
    var sts_check_stmt = snowflake.createStatement( {sqlText: sts_check_cmd} );
    var sts_check_res = sts_check_stmt.execute();
    sts_check_res.next();

    var v_is_invalid = sts_check_res.getColumnValue(1) > 0;
    var v_list_wrong_key = sts_check_res.getColumnValue(2);

    if (v_is_invalid) {
        var v_err_msg = "<SQLError>The following Dataset(s) is/are unkwnow " + v_list_wrong_key + " . you can't insert new data.</SQLError>";
        throw v_err_msg;
    }

    sts_check_cmd = `
SELECT   COUNT(*),LISTAGG(json.MAN_SCE_ELM_KEY,', ') list_
FROM     (SELECT   DISTINCT
                   REPLACE(X.ins_row:MAN_SCE_ELM_KEY,'"')                               AS MAN_SCE_ELM_KEY
          FROM     (SELECT parse_json(b.VALUE) as ins_row
                    FROM   (SELECT VALUE
                            FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                            WHERE  PATH LIKE '%InsertedRows%') a
                           ,LATERAL FLATTEN(a.VALUE) b) X) json
         INNER JOIN R_FLX_MAN_SCE ON
         (
             R_FLX_MAN_SCE.MAN_SCE_ELM_KEY  = json.MAN_SCE_ELM_KEY
         )
WHERE    MAN_SCE_USE_FLG = 1;
                    `;
    sts_check_stmt = snowflake.createStatement( {sqlText: sts_check_cmd} );
    sts_check_res = sts_check_stmt.execute();
    sts_check_res.next();

    v_is_invalid = sts_check_res.getColumnValue(1) > 0;
    v_list_wrong_key = sts_check_res.getColumnValue(2);

    if (v_is_invalid) {
        var v_err_msg = "<SQLError>The following Dataset(s) " + v_list_wrong_key + " is/are ready to use. You can't insert new data.</SQLError>";
        throw v_err_msg;
    }

    var ins_cmd = `
INSERT INTO F_FLX_MAN_SCE(MAN_SCE_ELM_KEY
                         ,MAN_SCE_ELM_COD
                         ,CBU_COD
                         ,IND_ELM_USR_DSC
                         ,PER_ELM_COD
                         ,ETI_ELM_COD
                         ,CUS_ELM_COD
                         ,PDT_ELM_COD
                         ,EIB_ELM_COD
                         ,TTY_ELM_COD
                         ,CAT_TYP_ELM_COD
                         ,DST_ELM_COD
                         ,ACC_ELM_COD
                         ,FCT_ARE_ELM_COD
                         ,AMOUNT)
SELECT   R_FLX_MAN_SCE.MAN_SCE_ELM_KEY
        ,R_FLX_MAN_SCE.MAN_SCE_ELM_COD
        ,R_FLX_MAN_SCE.CBU_COD
        ,REPLACE(X.ins_row:IND_ELM_USR_DSC,'"')                                    AS IND_ELM_USR_DSC
        ,LPAD(REPLACE(X.ins_row:PER_ELM_COD,'"'),2,'0')                            AS PER_ELM_COD
        ,REPLACE(X.ins_row:ETI_ELM_COD,'"')                                        AS ETI_ELM_COD
        ,REPLACE(X.ins_row:CUS_ELM_COD,'"')                                        AS CUS_ELM_COD
        ,REPLACE(X.ins_row:PDT_ELM_COD,'"')                                        AS PDT_ELM_COD
        ,COALESCE(NULLIF(REPLACE(X.ins_row:EIB_ELM_COD,'"'),''),'NA')              AS EIB_ELM_COD
        ,COALESCE(NULLIF(REPLACE(X.ins_row:TTY_ELM_COD,'"'),''),'NA')              AS TTY_ELM_COD
        ,COALESCE(NULLIF(REPLACE(X.ins_row:CAT_TYP_ELM_COD,'"'),''),'MGR')         AS CAT_TYP_ELM_COD
        ,REPLACE(X.ins_row:DST_ELM_COD,'"')                                        AS DST_ELM_COD
        ,REPLACE(X.ins_row:ACC_ELM_COD,'"')                                        AS ACC_ELM_COD
        ,REPLACE(X.ins_row:FCT_ARE_ELM_COD,'"')                                    AS FCT_ARE_ELM_COD
        ,COALESCE(REPLACE(X.ins_row:AMOUNT,'"'),'0')::NUMBER(32,12)                AS AMOUNT
--        ,REPLACE(X.ins_row:SAL_SUP_ELM_COD,'"')                                    AS SAL_SUP_ELM_COD
FROM     (SELECT parse_json(b.VALUE) as ins_row
                ,a.USER
          FROM   (SELECT VALUE
                        ,THIS:User AS USER
                  FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                  WHERE  PATH LIKE '%InsertedRows%') a
                 ,LATERAL FLATTEN(a.VALUE) b) X
         INNER JOIN R_FLX_MAN_SCE ON
         (
             R_FLX_MAN_SCE.MAN_SCE_ELM_KEY  = REPLACE(X.ins_row:MAN_SCE_ELM_KEY,'"')
         );
                    `;
    snowflake.execute( {sqlText: ins_cmd} );

    /* Update Line indicator and error flag */
    var upd_cmd = `
UPDATE COP_DMT_FLX.F_FLX_MAN_SCE
SET    IND_ELM_COD     = V_CHK_F_FLX_MAN_SCE.IND_ELM_COD
      ,ACC_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.ACC_ERR_FLG
      ,DST_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.DST_ERR_FLG
      ,FCT_ARE_ERR_FLG = V_CHK_F_FLX_MAN_SCE.FCT_ARE_ERR_FLG
      ,CUS_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.CUS_ERR_FLG
      ,PDT_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.PDT_ERR_FLG
      ,ETI_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.ETI_ERR_FLG
      ,EIB_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.EIB_ERR_FLG
      ,TTY_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.TTY_ERR_FLG
      ,CAT_TYP_ERR_FLG = V_CHK_F_FLX_MAN_SCE.CAT_TYP_ERR_FLG
      ,PER_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.PER_ERR_FLG
      ,MAN_ITM_ERR_FLG = V_CHK_F_FLX_MAN_SCE.MAN_ITM_ERR_FLG
      ,T_REC_UPD_TST   = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
FROM   COP_DMT_FLX.V_CHK_F_FLX_MAN_SCE
WHERE  F_FLX_MAN_SCE.ID              =  V_CHK_F_FLX_MAN_SCE.ID
AND    F_FLX_MAN_SCE.IND_ELM_COD     = '#ERR'
AND    F_FLX_MAN_SCE.MAN_ITM_ERR_FLG = 0;`;

    snowflake.execute( {sqlText: upd_cmd} );

    /* Update Line error text */
    upd_cmd = `
UPDATE COP_DMT_FLX.F_FLX_MAN_SCE
SET    MAN_ITM_ERR_DTA = '{' || RTRIM(
                         (CASE WHEN IND_ELM_COD     = '#ERR' THEN '"IND_ELM_COD":"Indicator ''' || IND_ELM_USR_DSC || ''' is not a base indicator",' ELSE '' END) ||
                         (CASE WHEN ACC_ERR_FLG     = 1      THEN '"ACC_ELM_COD":"Account ''' || ACC_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN DST_ERR_FLG     = 1      THEN '"DST_ELM_COD":"Destination ''' || DST_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN FCT_ARE_ERR_FLG = 1      THEN '"FCT_ARE_ELM_COD":"Functional Area ''' || FCT_ARE_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN CUS_ERR_FLG     = 1      THEN '"CUS_ELM_COD":"Customer ''' || CUS_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN PDT_ERR_FLG     = 1      THEN '"PDT_ELM_COD":"Product ''' || PDT_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN ETI_ERR_FLG     = 1      THEN '"ETI_ELM_COD":"Entity ''' || ETI_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN EIB_ERR_FLG     = 1      THEN '"EIB_ELM_COD":"Business Type ''' || EIB_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN TTY_ERR_FLG     = 1      THEN '"TTY_ELM_COD":"Territory ''' || TTY_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN CAT_TYP_ERR_FLG = 1      THEN '"CAT_TYP_ELM_COD":"Managerial/Internal transfers ''' || CAT_TYP_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN PER_ERR_FLG     = 1      THEN '"PER_ELM_COD":"Period ''' || PER_ELM_COD || ''' must be between 01 and 12",' ELSE '' END),',') || '}'
      ,T_REC_UPD_TST   = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
WHERE  MAN_ITM_ERR_FLG  > 0
AND    MAN_ITM_ERR_DTA IS NULL;`;

    snowflake.execute( {sqlText: upd_cmd} );

}
/* End of Insert Operation Block */

/* Update Operation Block */
if (isUpdated) {

    var sts_check_cmd = `
SELECT   COUNT(*),LISTAGG(json.MAN_SCE_ELM_KEY,', ') list_
FROM     (SELECT   DISTINCT
                   REPLACE(X.upd_row:MAN_SCE_ELM_KEY,'"')                               AS MAN_SCE_ELM_KEY
          FROM     (SELECT parse_json(b.VALUE:Updated) as upd_row
                    FROM   (SELECT VALUE
                            FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                            WHERE  PATH LIKE '%UpdatedRows%') a
                           ,LATERAL FLATTEN(a.VALUE) b) X) json
         LEFT OUTER JOIN R_FLX_MAN_SCE ON
         (
             R_FLX_MAN_SCE.MAN_SCE_ELM_KEY  = json.MAN_SCE_ELM_KEY
         )
WHERE    MAN_SCE_USE_FLG IS NULL;
                        `;
    var sts_check_stmt = snowflake.createStatement( {sqlText: sts_check_cmd} );
    var sts_check_res = sts_check_stmt.execute();
    sts_check_res.next();

    var v_is_invalid = sts_check_res.getColumnValue(1) > 0;
    var v_list_wrong_key = sts_check_res.getColumnValue(2);

    if (v_is_invalid) {
        var v_err_msg = "<SQLError>The following Dataset(s) is/are unkwnow " + v_list_wrong_key + " . you can't insert new data.</SQLError>";
        throw v_err_msg;
    }

    sts_check_cmd = `
SELECT   COUNT(*),LISTAGG(json.MAN_SCE_ELM_KEY,', ') list_
FROM     (SELECT   DISTINCT
                   REPLACE(X.upd_row:MAN_SCE_ELM_KEY,'"')                               AS MAN_SCE_ELM_KEY
          FROM     (SELECT parse_json(b.VALUE:Updated) as upd_row
                    FROM   (SELECT VALUE
                            FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                            WHERE  PATH LIKE '%UpdatedRows%') a
                           ,LATERAL FLATTEN(a.VALUE) b) X) json
         INNER JOIN R_FLX_MAN_SCE ON
         (
             R_FLX_MAN_SCE.MAN_SCE_ELM_KEY  = json.MAN_SCE_ELM_KEY
         )
WHERE    MAN_SCE_USE_FLG = 1;
                    `;
    sts_check_stmt = snowflake.createStatement( {sqlText: sts_check_cmd} );
    sts_check_res = sts_check_stmt.execute();
    sts_check_res.next();

    v_is_invalid = sts_check_res.getColumnValue(1) > 0;
    v_list_wrong_key = sts_check_res.getColumnValue(2);

    if (v_is_invalid) {
        var v_err_msg = "<SQLError>The following Dataset(s) " + v_list_wrong_key + " is/are ready to use. You can't insert new data.</SQLError>";
        throw v_err_msg;
    }

    var upd_cmd = `
UPDATE COP_DMT_FLX.F_FLX_MAN_SCE
SET    F_FLX_MAN_SCE.IND_ELM_USR_DSC = REPLACE(X.upd_row:IND_ELM_USR_DSC,'"')
      ,F_FLX_MAN_SCE.PER_ELM_COD     = REPLACE(X.upd_row:PER_ELM_COD,'"')
      ,F_FLX_MAN_SCE.ETI_ELM_COD     = REPLACE(X.upd_row:ETI_ELM_COD,'"')
      ,F_FLX_MAN_SCE.CUS_ELM_COD     = REPLACE(X.upd_row:CUS_ELM_COD,'"')
      ,F_FLX_MAN_SCE.PDT_ELM_COD     = REPLACE(X.upd_row:PDT_ELM_COD,'"')
      ,F_FLX_MAN_SCE.EIB_ELM_COD     = COALESCE(NULLIF(REPLACE(X.upd_row:EIB_ELM_COD,'"'),''),'NA') 
      ,F_FLX_MAN_SCE.CAT_TYP_ELM_COD = COALESCE(NULLIF(REPLACE(X.upd_row:CAT_TYP_ELM_COD,'"'),''),'MGR') 
      ,F_FLX_MAN_SCE.TTY_ELM_COD     = COALESCE(NULLIF(REPLACE(X.upd_row:TTY_ELM_COD,'"'),''),'NA') 
      ,F_FLX_MAN_SCE.DST_ELM_COD     = REPLACE(X.upd_row:DST_ELM_COD,'"')
      ,F_FLX_MAN_SCE.ACC_ELM_COD     = REPLACE(X.upd_row:ACC_ELM_COD,'"')
      ,F_FLX_MAN_SCE.FCT_ARE_ELM_COD = REPLACE(X.upd_row:FCT_ARE_ELM_COD,'"')
      ,F_FLX_MAN_SCE.AMOUNT          = COALESCE(REPLACE(X.upd_row:AMOUNT,'"'),'0')::NUMBER(32,12)
      ,F_FLX_MAN_SCE.IND_ELM_COD     = NULL
      ,F_FLX_MAN_SCE.ACC_ERR_FLG     = 0
      ,F_FLX_MAN_SCE.DST_ERR_FLG     = 0
      ,F_FLX_MAN_SCE.FCT_ARE_ERR_FLG = 0
      ,F_FLX_MAN_SCE.CUS_ERR_FLG     = 0
      ,F_FLX_MAN_SCE.PDT_ERR_FLG     = 0
      ,F_FLX_MAN_SCE.ETI_ERR_FLG     = 0
      ,F_FLX_MAN_SCE.EIB_ERR_FLG     = 0
      ,F_FLX_MAN_SCE.TTY_ERR_FLG     = 0
      ,F_FLX_MAN_SCE.CAT_TYP_ERR_FLG = 0
      ,F_FLX_MAN_SCE.PER_ERR_FLG     = 0
      ,F_FLX_MAN_SCE.MAN_ITM_ERR_FLG = 0
      ,F_FLX_MAN_SCE.MAN_ITM_ERR_DTA = NULL
      ,F_FLX_MAN_SCE.T_REC_UPD_TST   = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
--      ,F_FLX_MAN_SCE.SAL_SUP_ELM_COD = REPLACE(X.upd_row:SAL_SUP_ELM_COD,'"')
FROM   (SELECT parse_json(b.VALUE:Updated) as upd_row
              ,parse_json(b.VALUE:Original) as key_row
        FROM   (SELECT VALUE
                FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                WHERE  PATH LIKE '%UpdatedRows%') a
               ,LATERAL FLATTEN(a.VALUE) b) X
       INNER JOIN COP_DMT_FLX.F_FLX_MAN_SCE F_MAN_SCE ON
       (
           F_MAN_SCE.ID = DECODE(REPLACE(X.upd_row:ID,'"')
                                ,'NA',REPLACE(X.key_row:ID,'"')
                               ,REPLACE(X.upd_row:ID,'"'))
       )
	INNER JOIN COP_DMT_FLX.R_FLX_MAN_SCE ON
       (
           R_FLX_MAN_SCE.MAN_SCE_ELM_KEY = F_MAN_SCE.MAN_SCE_ELM_KEY AND
           R_FLX_MAN_SCE.MAN_SCE_USE_FLG = 0
       )
WHERE  F_FLX_MAN_SCE.ID = DECODE(REPLACE(X.upd_row:ID,'"')
                                ,'NA',REPLACE(X.key_row:ID,'"')
                                ,REPLACE(X.upd_row:ID,'"'));
                 `;

    snowflake.execute( {sqlText: upd_cmd} );

    /* Update Line indicator and error flag */
    upd_cmd = `
UPDATE COP_DMT_FLX.F_FLX_MAN_SCE
SET    IND_ELM_COD     = V_CHK_F_FLX_MAN_SCE.IND_ELM_COD
      ,ACC_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.ACC_ERR_FLG
      ,DST_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.DST_ERR_FLG
      ,FCT_ARE_ERR_FLG = V_CHK_F_FLX_MAN_SCE.FCT_ARE_ERR_FLG
      ,CUS_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.CUS_ERR_FLG
      ,PDT_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.PDT_ERR_FLG
      ,ETI_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.ETI_ERR_FLG
      ,EIB_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.EIB_ERR_FLG
      ,TTY_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.TTY_ERR_FLG
      ,CAT_TYP_ERR_FLG = V_CHK_F_FLX_MAN_SCE.CAT_TYP_ERR_FLG
      ,PER_ERR_FLG     = V_CHK_F_FLX_MAN_SCE.PER_ERR_FLG
      ,MAN_ITM_ERR_FLG = V_CHK_F_FLX_MAN_SCE.MAN_ITM_ERR_FLG
      ,T_REC_UPD_TST   = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
FROM   COP_DMT_FLX.V_CHK_F_FLX_MAN_SCE
WHERE  F_FLX_MAN_SCE.ID           =  V_CHK_F_FLX_MAN_SCE.ID
AND    F_FLX_MAN_SCE.IND_ELM_COD IS NULL;
                 `;

    snowflake.execute( {sqlText: upd_cmd} );

    /* Update Line error text */
    upd_cmd = `
UPDATE COP_DMT_FLX.F_FLX_MAN_SCE
SET    MAN_ITM_ERR_DTA = '{' || RTRIM(
                         (CASE WHEN IND_ELM_COD     = '#ERR' THEN '"IND_ELM_COD":"Indicator ''' || IND_ELM_USR_DSC || ''' is not a base indicator",' ELSE '' END) ||
                         (CASE WHEN ACC_ERR_FLG     = 1      THEN '"ACC_ELM_COD":"Account ''' || ACC_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN DST_ERR_FLG     = 1      THEN '"DST_ELM_COD":"Destination ''' || DST_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN FCT_ARE_ERR_FLG = 1      THEN '"FCT_ARE_ELM_COD":"Functional Area ''' || FCT_ARE_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN CUS_ERR_FLG     = 1      THEN '"CUS_ELM_COD":"Customer ''' || CUS_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN PDT_ERR_FLG     = 1      THEN '"PDT_ELM_COD":"Product ''' || PDT_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN ETI_ERR_FLG     = 1      THEN '"ETI_ELM_COD":"Entity ''' || ETI_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN EIB_ERR_FLG     = 1      THEN '"EIB_ELM_COD":"Business Type ''' || EIB_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN TTY_ERR_FLG     = 1      THEN '"TTY_ELM_COD":"Territory ''' || TTY_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN CAT_TYP_ERR_FLG = 1      THEN '"CAT_TYP_ELM_COD":"Managerial/Internal transferts ''' || CAT_TYP_ELM_COD || ''' does not exist in the masterdata",' ELSE '' END) ||
                         (CASE WHEN PER_ERR_FLG     = 1      THEN '"PER_ELM_COD":"Period ''' || PER_ELM_COD || ''' must be between 01 and 12",' ELSE '' END),',') || '}'
      ,T_REC_UPD_TST   = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
WHERE  MAN_ITM_ERR_FLG  > 0
AND    MAN_ITM_ERR_DTA IS NULL;
                 `;

    snowflake.execute( {sqlText: upd_cmd} );

}
/* End of Update Operation Block */

/* Delete Operation Block */
if (isDeleted) {
    /* Getting ID to DELETE */
    var del_cmd = `
UPDATE COP_DMT_FLX.F_FLX_MAN_SCE
SET    F_FLX_MAN_SCE.T_REC_DLT_FLG = 1
      ,F_FLX_MAN_SCE.T_REC_UPD_TST = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
FROM   (SELECT parse_json(b.VALUE) as del_row
        FROM   (SELECT VALUE
                FROM   LATERAL FLATTEN(parse_json('${JSON_INPUT}')) f1
                WHERE  PATH LIKE '%DeletedRows%') a
               ,LATERAL FLATTEN(a.VALUE) b) X
WHERE  ID = REPLACE(X.del_row:ID,'"');
                 `;
    snowflake.execute( {sqlText: del_cmd} );

}
/* End of Delete Operation Block */

$$
;
