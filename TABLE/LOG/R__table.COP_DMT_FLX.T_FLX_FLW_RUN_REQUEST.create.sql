USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE TABLE T_FLX_FLW_RUN_REQUEST (
	-- Run identification information
	ID_IS_RUN_REQUEST VARCHAR(256) NOT NULL COMMENT 'Run Request Identifier ID',
	DS_IS_RUN_SOFT VARCHAR(256) COMMENT 'ETL Software Name (can be Full qualified name of runtime environment for Informatica Cloud, SNOW for Snowflake, Informatica Mass Ingestion, …)',
	DS_IS_RUN_NAMESPACE VARCHAR(256) COMMENT 'Run Namespace (Folder for IICS, Schema for Stored Procedure, …)',
	DS_IS_RUN_FLOW VARCHAR(256) COMMENT 'Run Flow Name (will be the taskflow name for IICS, the main Snowflake procedure for Snowflake, …)',
	DS_IS_RUN_INSTANCE_NAME VARCHAR(256) DEFAULT '#' COMMENT 'Run Instance Name (when process is instanciated, contains the instance name, otherwise is equal to "#")',
	
	-- Run Request General information
	DS_IS_REQUEST VARCHAR(1000) COMMENT 'Run request description (like reason for additional run request)',
	CD_IS_REQUEST_TYPE VARCHAR(1000) COMMENT 'Run request type (can be FIXED_RANGE_TST, FUNC_KEY, ...)',
	DS_IS_REQUESTOR_USER VARCHAR(100) COMMENT 'Run requestor user',
	CD_IS_RUN_DATA_LOAD_STRATEGY VARCHAR(50) COMMENT 'Data Loading Strategy (can be DLT for Delta, REC for Recovery, REC-DLT for Recovery/Delta, INI for Init)',
	CD_IS_RUN_STATUS VARCHAR(30) COMMENT 'Run Status (can be PLANNED, CANCELLED, ONGOING, FAILED, SUCCESS)',
	
	-- Run Technical Recovery information
	DT_PULL_FROM TIMESTAMP_NTZ(9) COMMENT 'Pull Begin Timestamp',
	DT_PULL_TO TIMESTAMP_NTZ(9) COMMENT 'Pull End Timestamp',
	
	-- Run Functional Key Recovery information
	DS_FUNC_KEY VARCHAR(100000) COMMENT 'Request functional key information (if multiple entries, split by delimiter ",")',
	
	-- Run Functional Range Recovery information
	CD_DELTA_PERIOD_RANGE_FROM_TYPE VARCHAR(256) COMMENT 'Incremental Period Range From Type (can be DAY, MONTH, …)',
	DS_DELTA_PERIOD_RANGE_FROM VARCHAR(256) COMMENT 'Incremental Period Range From',
	CD_DELTA_PERIOD_RANGE_TO_TYPE VARCHAR(256) COMMENT 'Incremental Period Range To Type (can be DAY, MONTH, …)',
	DS_DELTA_PERIOD_RANGE_TO VARCHAR(256) COMMENT 'Incremental Period Range To',
	
	-- Technical fields
	DT_IS_INSERT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP COMMENT '[Technical] Timestamp of insertion of entry',
	DS_IS_INSERT_USER VARCHAR(100) DEFAULT CURRENT_USER COMMENT '[Technical] Name of the user who inserted this entry (user executing the flow)',
	DT_IS_UPDATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP COMMENT '[Technical] Timestamp of last modification of entry',
	DS_IS_UPDATE_USER VARCHAR(100) DEFAULT CURRENT_USER COMMENT '[Technical] Name of the user who requested the last modification on entry (user executing the flow)',
	
	CONSTRAINT PK_T_FLX_FLW_RUN_REQUEST PRIMARY KEY (ID_IS_RUN_REQUEST)
) COMMENT = '[Technical] Data Flow - Run request table'
;