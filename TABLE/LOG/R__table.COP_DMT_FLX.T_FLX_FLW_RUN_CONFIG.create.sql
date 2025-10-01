USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE T_FLX_FLW_RUN_CONFIG (
	-- Run identification information
	DS_IS_RUN_SOFT VARCHAR(256) COMMENT 'ETL Software Name (can be Full qualified name of runtime environment for Informatica Cloud, SNOW for Snowflake, Informatica Mass Ingestion, …)',
	DS_IS_RUN_NAMESPACE VARCHAR(256) COMMENT 'Run Namespace (Folder for IICS, Database/Schema for Stored Procedure, …)',
	DS_IS_RUN_FLOW VARCHAR(256) COMMENT 'Run Flow Name (will be the taskflow name for IICS, the main Snowflake procedure for Snowflake, …)',
	DS_IS_RUN_INSTANCE_NAME VARCHAR(256) DEFAULT '#' COMMENT 'Run Instance Name (when process is instanciated, contains the instance name, otherwise is equal to \"#\")',
	
	-- Run General information
	DS_TARGET_SCHEMA VARCHAR(50) COMMENT 'Target Schema Name',
	DS_TARGET_TABLE VARCHAR(50) COMMENT 'Target Table Name',
	CD_RUN_DATA_SELECTION_TYPE VARCHAR(30) COMMENT 'Process Data Selection Type (references to T_DTA_FLW_RUN_DATA_SELECTION_TYPE.CD_RUN_DATA_SELECTION_TYPE)',
	CD_RUN_DEFAULT_DATA_LOADING_STRATEGY VARCHAR(50) COMMENT 'Process Default Data Loading Strategy (can be DLT for Delta or FUL for Full)',
	
	-- Run Execution authorizations
	FL_RUN_FULL_REPLACE_AUTHORIZED NUMBER(1,0) COMMENT 'Full Replace Authorized flag (1=Yes, 0=No)',
	FL_RUN_FULL_UPSERT_DELETE_AUTHORIZED NUMBER(1,0) COMMENT 'Full Upsert/Delete Authorized flag (1=Yes, 0=No)',
	FL_RUN_DELTA_AUTHORIZED NUMBER(1,0) COMMENT 'Delta Authorized flag (1=Yes, 0=No)',
	FL_RUN_RECOVERY_AUTHORIZED NUMBER(1,0) COMMENT 'Recovery Authorized flag (1=Yes, 0=No)',
	FL_RUN_INIT_AUTHORIZED NUMBER(1,0) COMMENT 'Init Authorized flag (1=Yes, 0=No)',
	
	-- Run Technical Delta information
	CD_RUN_DELTA_PULL_TYPE VARCHAR(50) COMMENT 'Delta Pull Type Code (references to T_DTA_FLW_RUN_DELTA_PULL_TYPE.CD_RUN_DELTA_PULL_TYPE)',
	DS_PULL_FIELD_NAME VARCHAR(256) COMMENT 'Pull Field Name',
	DS_PULL_FIELD_FORMAT VARCHAR(256) COMMENT 'Pull Field Format',
	DS_TARGET_FIELD_NAME_FOR_SOURCE_LAST_UPDATE_TST VARCHAR(256) COMMENT 'Source Last Update Field Name in Target Table',
	
	-- Run Functional Delta information
	CD_DELTA_PERIOD_REFERENCE_TYPE VARCHAR(256) COMMENT 'Incremental Period Reference Type (can be DAY, MONTH, …)',
	DS_DELTA_PERIOD_REFERENCE VARCHAR(256) COMMENT 'Incremental Period Reference',
	CD_DELTA_PERIOD_RANGE_FROM_TYPE VARCHAR(256) COMMENT 'Incremental Period Range From Type (can be DAY, MONTH, …)',
	DS_DELTA_PERIOD_RANGE_FROM VARCHAR(256) COMMENT 'Incremental Period Range From',
	CD_DELTA_PERIOD_RANGE_TO_TYPE VARCHAR(256) COMMENT 'Incremental Period Range To Type (can be DAY, MONTH, …)',
	DS_DELTA_PERIOD_RANGE_TO VARCHAR(256) COMMENT 'Incremental Period Range To',
	
	-- Technical fields
	DT_IS_INSERT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP COMMENT '[Technical] Timestamp of insertion of entry',
	DS_IS_INSERT_USER VARCHAR(100) DEFAULT CURRENT_USER COMMENT '[Technical] Name of the user who inserted this entry (user executing the flow)',
	DT_IS_UPDATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP COMMENT '[Technical] Timestamp of last modification of entry',
	DS_IS_UPDATE_USER VARCHAR(100) DEFAULT CURRENT_USER COMMENT '[Technical] Name of the user who requested the last modification on entry (user executing the flow)',
	
	CONSTRAINT PK_T_FLX_FLW_RUN_CONFIG PRIMARY KEY (DS_IS_RUN_SOFT, DS_IS_RUN_NAMESPACE, DS_IS_RUN_FLOW)
) COMMENT = '[Technical] Data Flow - Run/Process configuration'
;