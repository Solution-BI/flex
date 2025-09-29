USE SCHEMA COP_DMT_FLX{{uid}};

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script For Logging Data From Power On
Author      : Noel Coquio (Solution BI France)                      
Created On  : 09-07-2024                                                 
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/



CREATE OR REPLACE PROCEDURE SP_Flex_Log_PowerOn(LOG_ID VARCHAR(255),CALLING_SP VARCHAR(255),JSON_INPUT VARCHAR(16777216),USER_EML VARCHAR(500))
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$

var cmd = `INSERT INTO T_FLX_LOG_MSG_JSN (SNW_QUERY_ID,SNW_PROC_NAME,JSON_TEXT,T_REC_INS_TST,T_REC_INS_USR) 
           SELECT DISTINCT
                  :1                                         AS SNW_QUERY_ID
                 ,:2                                         AS SNW_PROC_NAME
                 ,REPLACE(REPLACE(:3,'DOUBLEQUOTE','"')
                         ,'QUOTE','''')                      AS JSON_TEXT
                 ,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)        AS T_REC_INS_TST
                 ,:4                                         AS T_REC_INS_USR;
          `;
var statement = snowflake.createStatement( {sqlText: cmd
                                                   , binds:[
                                                           LOG_ID
                                                          ,CALLING_SP
                                                          ,JSON_INPUT
                                                          ,USER_EML
                                                          ]
                                           } 
                                         );

statement.execute();

return `${statement.getNumRowsAffected()} row(s) logged.`;
$$
;
