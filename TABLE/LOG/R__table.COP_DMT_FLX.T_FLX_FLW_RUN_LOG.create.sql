USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE T_FLX_FLW_RUN_LOG (
	-- Run Log identification information
	DS_IS_RUN_SOFT VARCHAR(256) COMMENT 'ETL Software Name (can be Full qualified name of runtime environment for Informatica Cloud, SNOW for Snowflake, Informatica Mass Ingestion, …)',
	ID_IS_RUN_FLOW VARCHAR(256) NOT NULL COMMENT 'Run Flow Identifier ID',
	DS_IS_RUN_NAMESPACE VARCHAR(256) COMMENT 'Run Namespace (Folder for IICS, Schema for Stored Procedure, …)',
	DS_IS_RUN_FLOW VARCHAR(256) COMMENT 'Run Flow Name (will be the taskflow name for IICS, the main Snowflake procedure for Snowflake, …)',
	DS_IS_RUN_INSTANCE_NAME VARCHAR(256) DEFAULT '#' COMMENT 'Run Instance Name (when process is instanciated, contains the instance name, otherwise is equal to "#")',
	DT_IS_RUN_BEGIN TIMESTAMP_NTZ(9) COMMENT 'Run Begin Timestamp (in GMT)',
	
	-- Run General information
	CD_IS_RUN_DATA_LOAD_STRATEGY VARCHAR(50) COMMENT 'Data Loading Strategy (can be DLT for Delta, REC for Recovery, INI for Init)',
	DT_IS_RUN_END TIMESTAMP_NTZ(9) COMMENT 'Run End Timestamp (in GMT)',
	CD_IS_RUN_STATUS VARCHAR(30) COMMENT 'Run Status (can be ONGOING, FAILED, SUCCESS)',
	ID_IS_RUN_REQUEST VARCHAR(256) COMMENT 'Run description (like reason for additional run request)',
	DS_IS_REQUESTOR_USER VARCHAR(100) COMMENT 'Run requestor user',
	DS_IS_LOG_FILE_PATH VARCHAR(10000) COMMENT 'Log File Path',
	
	-- Pull information fields
	DS_PULL_FIELD_NAME VARCHAR(256) COMMENT 'Pull Field Name',
	DT_PULL_FROM TIMESTAMP_NTZ(9) COMMENT 'Pull Begin Timestamp',
	DT_PULL_TO TIMESTAMP_NTZ(9) COMMENT 'Pull End Timestamp',
	DT_PULL_SOURCE_LST TIMESTAMP_NTZ(9) COMMENT 'Pull Actual Last Sourced Timestamp',
	DS_PULL_FILTER_DSC VARCHAR(10000) COMMENT 'Pull Filter description',

	-- Technical fields
	DT_IS_INSERT TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT '[Technical] Timestamp of insertion of entry',
	DS_IS_INSERT_USER VARCHAR(100) NOT NULL DEFAULT CURRENT_USER() COMMENT '[Technical] Name of the user who inserted this entry (user executing the flow)',
	DT_IS_UPDATE TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT '[Technical] Timestamp of last modification of entry',
	DS_IS_UPDATE_USER VARCHAR(100) NOT NULL DEFAULT CURRENT_USER() COMMENT '[Technical] Name of the user who requested the last modification on entry (user executing the flow)',
	
	CONSTRAINT PK_T_FLX_FLW_RUN_LOG PRIMARY KEY (DS_IS_RUN_SOFT, ID_IS_RUN_FLOW)
) COMMENT = '[Technical] Data Flow - Run Log - Run level information'
;