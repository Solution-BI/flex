USE SCHEMA COP_DMT_FLX;

/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Table Script For Table T_FLX_ADT_LOG__ARCHIVE
Author      : Noel COQUIO (Solution BI France)                      
Created On  : 22-10-2024
=========================================================================
Modified On:      Description:                        Author:          
=========================================================================
*/

CREATE OR REPLACE TABLE T_FLX_ADT_LOG__ARCHIVE
      (ID                 NUMBER          NOT NULL    COMMENT 'Internal ID of the log'
      ,SCE_ELM_KEY        VARCHAR(64)                 COMMENT 'Scenario Key : concatenation of CBU and Scenario Code'
      ,CBU_COD            VARCHAR(10)                 COMMENT 'CBU/Market'
      ,SCE_ELM_COD        VARCHAR(50)                 COMMENT 'Scenario code'
      ,ETI_ELM_LIS_TXT    VARCHAR(255)                COMMENT 'List of the Entity impacted'
      ,CUS_ELM_LIS_TXT    VARCHAR                     COMMENT 'List of the Customer impacted'
      ,PDT_ELM_LIS_TXT    VARCHAR                     COMMENT 'List of the Product impacted'
      ,EIB_ELM_LIS_TXT    VARCHAR                     COMMENT 'List of the Entity impacted'
      ,TTY_ELM_LIS_TXT    VARCHAR(255)                COMMENT 'List of the Territory impacted'
      ,IND_ELM_COD        VARCHAR(30)                 COMMENT 'Indicator Code impacted'
      ,IND_ELM_DSC        VARCHAR(255)                COMMENT 'Indicator Name impacted'
      ,PER_ELM_LIS_TXT    VARCHAR(255)                COMMENT 'List of the Period impacted'
      ,OLD_VAL            NUMBER(32,12)               COMMENT 'Value of the indicator before change'
      ,NEW_VAL            NUMBER(32,12)               COMMENT 'Value of the indicatore after change'
      ,IPC_VAL            NUMBER(32,12)               COMMENT 'Impact value for the indicator'
      ,IPC_VAL_PCT        NUMBER(32,12)               COMMENT 'Percentage of Impact for the indicator'
      ,IPC_CMT_TXT        VARCHAR                     COMMENT 'Comment'
      ,T_REC_UPD_USR      VARCHAR(255)                COMMENT 'User requesting the change'
      ,T_REC_UPD_TST      TIMESTAMP_TZ                COMMENT '[Technical] Timestamp of last update into the table'
      ,CONSTRAINT PK_T_FLX_ADT_LOG__ARCHIVE PRIMARY KEY (ID)
      ) COMMENT = '[Flex] Flex Audit Log table coming from SqlServer';
