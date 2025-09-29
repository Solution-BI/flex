USE SCHEMA COP_DMT_FLX{{uid}};

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Table Script For Table T_FLX_LOG_POWERON
Author      : Noel COQUIO (Solution BI France)                      
Created On  : 05-07-2024
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

CREATE OR REPLACE TABLE T_FLX_LOG_MSG_JSN
         (SNW_QUERY_ID            VARCHAR(256)        COMMENT 'Snowflake Query Id'
         ,SNW_PROC_NAME           VARCHAR(256)        COMMENT 'Name of calling stored procedure'
         ,JSON_TEXT               VARCHAR             COMMENT 'JSON strings passed by PowerOn'
         ,T_REC_INS_TST           TIMESTAMP_NTZ       COMMENT 'Timestamp of record creation'
         ,T_REC_INS_USR           VARCHAR(100)        COMMENT 'User who created the record'
         ) COMMENT = '[Flex] Log table for all the JSON messages sent by PowerOn';

