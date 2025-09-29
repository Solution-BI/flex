USE SCHEMA COP_DMT_FLX{{uid}};

CREATE OR REPLACE TABLE T_FLX_FLW_RUN_STP_LOG (
	-- Run Log identification information
	DS_IS_RUN_SOFT VARCHAR(256) NOT NULL COMMENT 'ETL Software Name (can be Full qualified name of runtime environment for Informatica Cloud, SNOW for Snowflake, Informatica Mass Ingestion, …)',
	ID_IS_RUN_FLOW VARCHAR(256) NOT NULL COMMENT 'Run Flow Identifier ID',
	DS_IS_RUN_STP_NAM_DSC VARCHAR(256) NOT NULL COMMENT 'Run Step Name',
	ID_IS_RUN_STP_SUB_ORD_VAL NUMERIC(10, 0) NOT NULL COMMENT 'Run Step Sub Order (when multiple queries in same step / default value is 1)',

	-- Step General Information
	CD_IS_STEP_STATUS_VALUE NUMBER(38, 0) COMMENT 'Step Status Value (0 when in success, negative when error and positive when in warning)',
	DS_IS_STEP_ERROR VARCHAR(10000) COMMENT 'Step Error Message (filled only when step is in Failure)',
	DT_IS_STEP_BEGIN TIMESTAMP_NTZ(9) COMMENT 'Run Step Timestamp (in GMT)',
	DT_IS_STEP_END TIMESTAMP_NTZ(9) COMMENT 'Run Step Timestamp (in GMT)',
	DS_IS_QUERY VARCHAR(10000) COMMENT 'Query executed on step',
	
	-- Step Read/Write information fields
	MT_SRC_ROW_SUC NUMERIC(38, 0) COMMENT 'Number of Source Rows read (in Success)',
	MT_SRC_ROW_ERR NUMERIC(38, 0) COMMENT 'Number of Source Rows read (in Failure)',
	MT_TGT_ROW_INS_SUC NUMERIC(38, 0) COMMENT 'Number of Target Rows inserted (in Success)',
	MT_TGT_ROW_INS_ERR NUMERIC(38, 0) COMMENT 'Number of Target Rows inserted (Rejected)',
	MT_TGT_ROW_UPD_SUC NUMERIC(38, 0) COMMENT 'Number of Target Rows updated (in Success)',
	MT_TGT_ROW_UPD_ERR NUMERIC(38, 0) COMMENT 'Number of Target Rows updated (Rejected)',
	MT_TGT_ROW_DEL_SUC NUMERIC(38, 0) COMMENT 'Number of Target Rows deleted (in Success)',
	MT_TGT_ROW_DEL_ERR NUMERIC(38, 0) COMMENT 'Number of Target Rows deleted (Rejected)',

	-- Technical fields
	DT_IS_INSERT TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT '[Technical] Timestamp of insertion of entry',
	DS_IS_INSERT_USER VARCHAR(100) NOT NULL DEFAULT CURRENT_USER() COMMENT '[Technical] Name of the user who inserted this entry',
	DT_IS_UPDATE TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT '[Technical] Timestamp of last modification of entry',
	DS_IS_UPDATE_USER VARCHAR(100) NOT NULL DEFAULT CURRENT_USER() COMMENT '[Technical] Name of the user who requested the last modification on entry',
	
	CONSTRAINT PK_T_FLX_FLW_RUN_STP_LOG PRIMARY KEY (DS_IS_RUN_SOFT, ID_IS_RUN_FLOW, DS_IS_RUN_STP_NAM_DSC, ID_IS_RUN_STP_SUB_ORD_VAL)
) COMMENT = '[Technical] Data Flow - Run Log - Step level information' 
;