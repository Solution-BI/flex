USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE T_FLX_FLW_RUN_TABLE_CONFIG (
	-- Run General information
	DS_TARGET_DATABASE VARCHAR(100) COMMENT 'Target Database Name',
	DS_TARGET_SCHEMA VARCHAR(100) COMMENT 'Target Schema Name',
	DS_TARGET_TABLE VARCHAR(100) COMMENT 'Target Table Name',
	DS_SCOPE VARCHAR(100) DEFAULT '#' COMMENT 'Scope of the load (defaulted to \"#\")',

	-- Run Flow Last Information
	FL_LAST_FLOW NUMBER(1,0) COMMENT 'Is the last flow flag (1=Yes, 0=No)',
	
	-- Run identification information
	DS_IS_RUN_SOFT VARCHAR(256) COMMENT 'ETL Software Name (can be Full qualified name of runtime environment for Informatica Cloud, SNOW for Snowflake, Informatica Mass Ingestion, …)',
	DS_IS_RUN_NAMESPACE VARCHAR(256) COMMENT 'Run Namespace (Folder for IICS, Database/Schema for Stored Procedure, …)',
	DS_IS_RUN_FLOW VARCHAR(256) COMMENT 'Run Flow Name (will be the taskflow name for IICS, the main Snowflake procedure for Snowflake, …)',
	DS_IS_RUN_INSTANCE_NAME VARCHAR(256) DEFAULT '#' COMMENT 'Run Instance Name (when process is instanciated, contains the instance name, otherwise is equal to \"#\")',
	DS_IS_RUN_STP_NAM_DSC VARCHAR(256) NOT NULL COMMENT 'Run Step Name',

	-- Technical fields
	DT_IS_INSERT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP COMMENT '[Technical] Timestamp of insertion of entry',
	DS_IS_INSERT_USER VARCHAR(100) DEFAULT CURRENT_USER COMMENT '[Technical] Name of the user who inserted this entry (user executing the flow)',
	DT_IS_UPDATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP COMMENT '[Technical] Timestamp of last modification of entry',
	DS_IS_UPDATE_USER VARCHAR(100) DEFAULT CURRENT_USER COMMENT '[Technical] Name of the user who requested the last modification on entry (user executing the flow)'
	
) COMMENT = '[Technical] Data Flow - Table Run/Process association'
;