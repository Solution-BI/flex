USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE T_FLX_FLW_RUN_LOG_ADD (
	DS_IS_RUN_SOFT VARCHAR(256) NOT NULL COMMENT 'ETL Software Name (can be Full qualified name of runtime environment for Informatica Cloud, SNOW for Snowflake, Informatica Mass Ingestion, …)',
	ID_IS_RUN_FLOW VARCHAR(256) NOT NULL COMMENT 'Run Flow Identifier ID',
	CD_RUN_ADD_INFO VARCHAR(256) COMMENT 'Additional information code',
	DS_RUN_ADD_INFO VARCHAR(10000) COMMENT 'Additional information description',
	
	-- Technical fields
	DT_IS_INSERT TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT '[Technical] Timestamp of insertion of entry',
	DS_IS_INSERT_USER VARCHAR(100) NOT NULL DEFAULT CURRENT_USER() COMMENT '[Technical] Name of the user who inserted this entry (user executing the flow)',
	DT_IS_UPDATE TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT '[Technical] Timestamp of last modification of entry',
	DS_IS_UPDATE_USER VARCHAR(100) NOT NULL DEFAULT CURRENT_USER() COMMENT '[Technical] Name of the user who requested the last modification on entry (user executing the flow)',
	
	CONSTRAINT PK_T_FLX_FLW_RUN_LOG_ADD PRIMARY KEY (DS_IS_RUN_SOFT, ID_IS_RUN_FLOW, CD_RUN_ADD_INFO, DS_RUN_ADD_INFO)
) COMMENT = '[Technical] Data Flow - Run Log - Run additional information (specific to each run)' 
;