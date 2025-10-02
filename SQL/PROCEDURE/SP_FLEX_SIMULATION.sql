/****** Object:  StoredProcedure [flex].[SP_FLEX_SIMULATION]    Script Date: 10/2/2025 9:04:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE   PROCEDURE [flex].[SP_FLEX_SIMULATION] (@JSONText NVARCHAR(MAX))
AS
BEGIN
	DECLARE @RUN_ID        BIGINT       = CURRENT_TRANSACTION_ID();
	DECLARE @PROCESS_NAME  VARCHAR(128) = OBJECT_NAME(@@PROCID);
	-- This table will hold the number of records affected by the MERGE operation
	DECLARE @MERGE_LOG     TABLE (
		[SCE_ELM_KEY]   VARCHAR(64),
		[TGT_IND_CSV]   VARCHAR(MAX),
		[MERGE_ACTION]  NVARCHAR(10)
	)

	DECLARE @ERRNO__WRITE_ON_CLOSED_PERIOD  INT = 50001;
	DECLARE @ERRNO__DATABASE_DEADLOCK  INT = 50002;

    -- Deadlock management variables
    DECLARE @VAR_WHILE int = 0;
    DECLARE @VAR_CATCH int = 0;
    DECLARE @delay varchar(8);
    DECLARE @Is_Locked INT;

	/* Log start of Simulation */
	INSERT INTO [FLEX].[T_FLX_LOG] (RUN_ID, PROCESS_NAME, EVENT_TYPE) VALUES (@RUN_ID, @PROCESS_NAME, 'start');

    /* Deadlock Management on all the tables */
    SELECT @Is_Locked = COUNT(*)
    FROM    sys.dm_tran_locks L
            JOIN sys.partitions P ON P.hobt_id = L.resource_associated_entity_id
            JOIN sys.objects O ON O.object_id = P.object_id
    WHERE   resource_database_id = db_id()
	AND     O.Name IN (N'[T_FLX_PAR_LOG]',N'[T_FLX_LOG_MSG_JSN]');

    IF @Is_Locked > 0
	    -- Log the information of the deadlock (Table, transaction ID, Session ID)
        INSERT INTO [FLEX].[T_FLX_LOG_DEADLOCK] ([object_name], [run_id], [session_id])
        SELECT  O.Name               AS object_name
               ,TST.transaction_id   AS run_id
               ,L.request_session_id AS ession_id
        FROM    sys.dm_tran_locks L
                JOIN sys.partitions P ON P.hobt_id = L.resource_associated_entity_id
                JOIN sys.objects O ON O.object_id = P.object_id
                JOIN sys.dm_exec_sessions ES ON ES.session_id = L.request_session_id
                JOIN sys.dm_tran_session_transactions TST ON ES.session_id = TST.session_id
        WHERE   resource_database_id = db_id()
	    AND     O.Name IN (N'[T_FLX_PAR_LOG]',N'[T_FLX_LOG_MSG_JSN]');

    SET @delay = '00:00:01';
	SET @VAR_WHILE = 0;
    -- loop 3 times to wait the unlock. The delay increases 1, 2 and 4 seconds
	-- the loop end if the unlock occurs
	WHILE( @VAR_WHILE < 3 
           AND @Is_Locked > 0)
    BEGIN
        SET @VAR_WHILE += 1;

		INSERT INTO [flex].[t_flx_log] (run_id, process_name, event_type, event_payload) values (@RUN_ID, @PROCESS_NAME, 'deadlock', json_object('waitfor': @delay +' seconds'));
	    WAITFOR DELAY @delay;

        SELECT @Is_Locked = COUNT(*)
        FROM    sys.dm_tran_locks L
                JOIN sys.partitions P ON P.hobt_id = L.resource_associated_entity_id
                JOIN sys.objects O ON O.object_id = P.object_id
        WHERE   resource_database_id = db_id()
        AND     O.Name IN (N'[T_FLX_PAR_LOG]',N'[T_FLX_LOG_MSG_JSN]');

        SET @delay = '00:00:' + RIGHT(CONCAT('0', @VAR_WHILE * 2), 2);

    END
	SET @VAR_WHILE = 0;
	-- An error message is send if there is still tables locked
	IF @Is_Locked > 0
	BEGIN
		INSERT INTO [flex].[t_flx_log] (run_id, process_name, event_type, event_payload) values (@RUN_ID, @PROCESS_NAME, 'error', json_object('error': 'DEADLOCK'));
		THROW @ERRNO__DATABASE_DEADLOCK
		    , N'<SQLError>Your modifications can not be applied due to database deadlock. Please, retry in few seconds.</SQLError>'
			, 0;
	END

	/* Log the JSON payload */
	-- Only store the JSON payload if the procedure is configured in DEBUG mode
	INSERT INTO [flex].[T_FLX_LOG_MSG_JSN] ([SQL_TRANSACTION_ID], [SQL_PROC_NAME], [JSONTEXT], [T_REC_INS_TST], [T_REC_INS_USR])
		SELECT @RUN_ID, @PROCESS_NAME, CASE WHEN [DEBUG_FLG] = 1 THEN @JSONText END, GETUTCDATE(), NULL
		FROM [flex].[T_FLX_PAR_LOG]
		WHERE [SQL_PROC_NAME] = @PROCESS_NAME;

	/* Log metadata of the simulation */
	INSERT INTO [FLEX].[T_FLX_LOG] (RUN_ID, PROCESS_NAME, EVENT_TYPE, EVENT_PAYLOAD)
		SELECT
			@RUN_ID,
			@PROCESS_NAME,
			'parse_metadata' AS EVENT_TYPE,
			json_object(
				'target': lower(TARGET_SCHEMA + '.' + TARGET_TABLE),
				'user_email': lower(USER_EMAIL)
			)
		FROM OPENJSON(@JSONText) WITH (
			TARGET_TABLE   VARCHAR(256)  '$."Table"',
			TARGET_SCHEMA  VARCHAR(256)  '$."Schema"',
			USER_EMAIL     VARCHAR(256)  '$."User"'
		);

	/* Parse the JSON payload */
	SELECT
		[ID],
		[SCE_ELM_KEY],
		[PER_ELM_COD],
		[PER_OPN_FLG],
		[ETI_ELM_KEY],
		[CUS_ELM_KEY],
		[PDT_ELM_KEY],
		[EIB_ELM_KEY],
		[TTY_ELM_KEY],
		[SAL_SUP_ELM_KEY],
		[CAT_TYP_ELM_KEY],

		/* Old/New values for each KPI + direct increment (increment on numerator for ratios) */

		-- Volume sold
		[VL1000_OLD],
		[VL1000_NEW],
		ISNULL([VL1000_NEW], 0) - ISNULL([VL1000_OLD], 0) AS [VL1000_I],

		-- Net Sales (NS)
		[TL2030_OLD],
		[TL2030_NEW],
		ISNULL([TL2030_NEW], 0) - ISNULL([TL2030_OLD], 0) AS [TL2030_I],
		[V_TL2030],  -- Variability (sensitivity to Volume)

		-- Net Sales / Volume sold
		[TL2930_OLD],
		[TL2930_NEW],
		IIF(ISNULL([TL2930_OLD], 0) = ISNULL([TL2930_NEW], 0), 0,
			ISNULL([VL1000_NEW] * [TL2930_NEW] - [TL2030_OLD], 0)) AS [TL2030_I__TL2930],  -- Increment on Net Sales coming from an impact on NS / Vol

		-- Material Cost of Sales
		[CG3001_OLD],
		[CG3001_NEW],
		ISNULL([CG3001_NEW], 0) - ISNULL([CG3001_OLD], 0) AS [CG3001_I],
		[V_CG3001],  -- Variability (sensitivity to Volume)

		-- Material Cost of Sales / Volume sold
		[CG3901_OLD],
		[CG3901_NEW],
		IIF(ISNULL([CG3901_OLD], 0) = ISNULL([CG3901_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3901_NEW] - [CG3001_OLD], 0)) AS [CG3001_I__CG3901],  -- Increment on Material Cost of Sales coming from an impact on Material Cost of Sales / Vol

		-- Rest of Material Costs
		[CG3002_OLD],
		[CG3002_NEW],
		ISNULL([CG3002_NEW], 0) - ISNULL([CG3002_OLD], 0) AS [CG3002_I],
		IIF([CG3002_OLD] = 0, 0, [A_CG3002]) AS [A_CG3002],  -- FCA (not applied if the base value is zero)
		[V_CG3002],  -- Variability (sensitivity to Volume)

		-- Rest of Material Costs / Volume sold
		[CG3902_OLD],
		[CG3902_NEW],
		IIF(ISNULL([CG3902_OLD], 0) = ISNULL([CG3902_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3902_NEW] - [CG3002_OLD], 0)) AS [CG3002_I__CG3902],  -- Increment on Rest of Material Costs coming from an impact on Rest of Material Costs / Vol

		-- Material Costs
		[CG3000_OLD],
		[CG3000_NEW],
		ISNULL([CG3000_NEW], 0) - ISNULL([CG3000_OLD], 0) AS [CG3000_I],

		-- Material Costs / Volume sold
		[CG3900_OLD],
		[CG3900_NEW],
		IIF(ISNULL([CG3900_OLD], 0) = ISNULL([CG3900_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3900_NEW] - [CG3000_OLD], 0)) AS [CG3000_I__CG3900],  -- Increment on Material Costs coming from an impact on Mat. Costs / Vol

		-- Manufacturing Cost of Sales
		[CG3011_OLD],
		[CG3011_NEW],
		ISNULL([CG3011_NEW], 0) - ISNULL([CG3011_OLD], 0) AS [CG3011_I],
		[V_CG3011],  -- Variability (sensitivity to Volume)

		-- Manufacturing Cost of Sales / Volume sold
		[CG3911_OLD],
		[CG3911_NEW],
		IIF(ISNULL([CG3911_OLD], 0) = ISNULL([CG3911_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3911_NEW] - [CG3011_OLD], 0)) AS [CG3011_I__CG3911],  -- Increment on Manuf. Cost of Sales coming from an impact on Manuf. Cost of Sales / Vol

		-- Rest of Manufacturing Costs
		[CG3012_OLD],
		[CG3012_NEW],
		ISNULL([CG3012_NEW], 0) - ISNULL([CG3012_OLD], 0) AS [CG3012_I],
		IIF([CG3012_OLD] = 0, 0, [A_CG3012]) AS [A_CG3012],  -- FCA (not applied if the base value is zero)
		[V_CG3012],  -- Variability (sensitivity to Volume)

		-- Rest of Manufacturing Costs / Volume sold
		[CG3912_OLD],
		[CG3912_NEW],
		IIF(ISNULL([CG3912_OLD], 0) = ISNULL([CG3912_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3912_NEW] - [CG3012_OLD], 0)) AS [CG3012_I__CG3912],  -- Increment on Rest of Manuf. Costs coming from an impact on Rest of Manuf. Costs / Vol

		-- Manufacturing Costs
		[CG3010_OLD],
		[CG3010_NEW],
		ISNULL([CG3010_NEW], 0) - ISNULL([CG3010_OLD], 0) AS [CG3010_I],

		-- Manufacturing Costs / Volume sold
		[CG3910_OLD],
		[CG3910_NEW],
		IIF(ISNULL([CG3910_OLD], 0) = ISNULL([CG3910_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3910_NEW] - [CG3010_OLD], 0)) AS [CG3010_I__CG3910],  -- Increment on Manufacturing Costs coming from an impact on Manuf. Costs / Vol

		-- Freight to Customers and Internal Freight Out
		[CG3021_OLD],
		[CG3021_NEW],
		ISNULL([CG3021_NEW], 0) - ISNULL([CG3021_OLD], 0) AS [CG3021_I],
		[V_CG3021],  -- Variability (sensitivity to Volume)

		-- Freight to Customers and Internal Freight Out / Volume sold
		[CG3921_OLD],
		[CG3921_NEW],
		IIF(ISNULL([CG3921_OLD], 0) = ISNULL([CG3921_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3921_NEW] - [CG3021_OLD], 0)) AS [CG3021_I__CG3921],  -- Increment on FTC & IFO coming from an impact on FTC & IFO / Vol

		-- Unsaleable
		[CG3022_OLD],
		[CG3022_NEW],
		ISNULL([CG3022_NEW], 0) - ISNULL([CG3022_OLD], 0) AS [CG3022_I],
		[V_CG3022],  -- Variability (sensitivity to Volume)

		-- Unsaleable / Volume sold
		[CG3922_OLD],
		[CG3922_NEW],
		IIF(ISNULL([CG3922_OLD], 0) = ISNULL([CG3922_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3922_NEW] - [CG3022_OLD], 0)) AS [CG3022_I__CG3922],  -- Increment on Unsaleable coming from an impact on Unsaleable / Vol

		-- Rest of Logistic Costs
		[CG3023_OLD],
		[CG3023_NEW],
		ISNULL([CG3023_NEW], 0) - ISNULL([CG3023_OLD], 0) AS [CG3023_I],
		IIF([CG3023_OLD] = 0, 0, [A_CG3023]) AS [A_CG3023],  -- FCA (not applied if the base value is zero)
		[V_CG3023],  -- Variability (sensitivity to Volume)

		-- Rest of Logistic Costs / Volume sold
		[CG3923_OLD],
		[CG3923_NEW],
		IIF(ISNULL([CG3923_OLD], 0) = ISNULL([CG3923_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3923_NEW] - [CG3023_OLD], 0)) AS [CG3023_I__CG3923],  -- Increment on Rest of Log. Costs coming from an impact on Rest of Log. Costs / Vol

		-- Logistic Costs
		[CG3020_OLD],
		[CG3020_NEW],
		ISNULL([CG3020_NEW], 0) - ISNULL([CG3020_OLD], 0) AS [CG3020_I],

		-- Logistic Costs / Volume sold
		[CG3920_OLD],
		[CG3920_NEW],
		IIF(ISNULL([CG3920_OLD], 0) = ISNULL([CG3920_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3920_NEW] - [CG3020_OLD], 0)) AS [CG3020_I__CG3920],  -- Increment on Logistic Costs coming from an impact on Log. Costs / Vol

		-- Total COGS (Cost of Goods Sold)
		[CG3030_OLD],
		[CG3030_NEW],
		ISNULL([CG3030_NEW], 0) - ISNULL([CG3030_OLD], 0) AS [CG3030_I],

		-- Cost of Goods Sold (COGS) / Volume sold
		[CG3930_OLD],
		[CG3930_NEW],
		IIF(ISNULL([CG3930_OLD], 0) = ISNULL([CG3930_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3930_NEW] - [CG3030_OLD], 0)) AS [CG3030_I__CG3930],  -- Increment on COGS coming from an impact on COGS / Vol

		-- Gross Profit
		[CG3040_OLD],
		[CG3040_NEW],
		ISNULL([CG3040_NEW], 0) - ISNULL([CG3040_OLD], 0) AS [CG3040_I],

		-- Gross margin %
		[CG3740_OLD],
		[CG3740_NEW],
		IIF(ISNULL([CG3740_OLD], 0) = ISNULL([CG3740_NEW], 0), 0, 
			ISNULL([TL2030_NEW] * [CG3740_NEW] - [CG3040_OLD], 0)) AS [CG3040_I__CG3740],  -- Increment on Gross Profit coming from an impact on Gross Margin

		-- GP / Vol
		[CG3940_OLD],
		[CG3940_NEW],
		IIF(ISNULL([CG3940_OLD], 0) = ISNULL([CG3940_NEW], 0), 0, 
			ISNULL([VL1000_NEW] * [CG3940_NEW] - [CG3040_OLD], 0)) AS [CG3040_I__CG3940],  -- Increment on Gross Profit coming from an impact on GP / Vol

		-- A&P Working
		[AP4001_OLD],
		[AP4001_NEW],
		ISNULL([AP4001_NEW], 0) - ISNULL([AP4001_OLD], 0) AS [AP4001_I],

		-- A&P Working / Total A&P (%)
		[AP4501_OLD],
		[AP4501_NEW],
		IIF(ISNULL([AP4501_OLD], 0) = ISNULL([AP4501_NEW], 0), 0, 
			ISNULL([AP4000_NEW] * [AP4501_NEW] - [AP4001_OLD], 0)) AS [AP4001_I__AP4501],  -- Increment on A&P Working coming from an impact on A&P Working / A&P

		-- A&P Non Working
		[AP4002_OLD],
		[AP4002_NEW],
		ISNULL([AP4002_NEW], 0) - ISNULL([AP4002_OLD], 0) AS [AP4002_I],

		-- A&P Others
		[AP4003_OLD],
		[AP4003_NEW],
		ISNULL([AP4003_NEW], 0) - ISNULL([AP4003_OLD], 0) AS [AP4003_I],

		-- Marketing Costs (A&P)
		[AP4000_OLD],
		[AP4000_NEW],
		ISNULL([AP4000_NEW], 0) - ISNULL([AP4000_OLD], 0) AS [AP4000_I],

		-- A&P / NS (%)
		[AP4700_OLD],
		[AP4700_NEW],
		IIF(ISNULL([AP4700_OLD], 0) = ISNULL([AP4700_NEW], 0), 0, 
			ISNULL([TL2030_NEW] * [AP4700_NEW] - [AP4000_OLD], 0)) AS [AP4000_I__AP4700],  -- Increment on A&P coming from an impact on A&P / NS

		-- Product Margin (PM)
		[AP4010_OLD],
		[AP4010_NEW],
		ISNULL([AP4010_NEW], 0) - ISNULL([AP4010_OLD], 0) AS [AP4010_I],

		-- PM / NS (%)
		[AP4710_OLD],
		[AP4710_NEW],
		IIF(ISNULL([AP4710_OLD], 0) = ISNULL([AP4710_NEW], 0), 0, 
			ISNULL([TL2030_NEW] * [AP4710_NEW] - [AP4010_OLD], 0)) AS [AP4010_I__AP4710],  -- Increment on PM coming from an impact on PM / NS

		-- Sales Force Costs (SF)
		[SF5000_OLD],
		[SF5000_NEW],
		ISNULL([SF5000_NEW], 0) - ISNULL([SF5000_OLD], 0) AS [SF5000_I],

		-- SF / NS (%)
		[SF5700_OLD],
		[SF5700_NEW],
		IIF(ISNULL([SF5700_OLD], 0) = ISNULL([SF5700_NEW], 0), 0, 
			ISNULL([TL2030_NEW] * [SF5700_NEW] - [SF5000_OLD], 0)) AS [SF5000_I__SF5700],  -- Increment on SFC coming from an impact on SFC / NS

		-- Channel Margin (CM)
		[SF5010_OLD],
		[SF5010_NEW],
		ISNULL([SF5010_NEW], 0) - ISNULL([SF5010_OLD], 0) AS [SF5010_I],

		-- CM / NS (%)
		[SF5710_OLD],
		[SF5710_NEW],
		IIF(ISNULL([SF5710_OLD], 0) = ISNULL([SF5710_NEW], 0), 0, 
			ISNULL([TL2030_NEW] * [SF5710_NEW] - [SF5010_OLD], 0)) AS [SF5010_I__SF5710],  -- Increment on CM coming from an impact on CM / NS

		-- HOO Market excluding OPS
		[HO5051_OLD],
		[HO5051_NEW],
		ISNULL([HO5051_NEW], 0) - ISNULL([HO5051_OLD], 0) AS [HO5051_I],

		-- HOO Operations
		[HO5052_OLD],
		[HO5052_NEW],
		ISNULL([HO5052_NEW], 0) - ISNULL([HO5052_OLD], 0) AS [HO5052_I],

		-- HOO DBS
		[HO5053_OLD],
		[HO5053_NEW],
		ISNULL([HO5053_NEW], 0) - ISNULL([HO5053_OLD], 0) AS [HO5053_I],

		-- HOO Global functions
		[HO5054_OLD],
		[HO5054_NEW],
		ISNULL([HO5054_NEW], 0) - ISNULL([HO5054_OLD], 0) AS [HO5054_I],

		-- Head Office Overheads (HOO)
		[HO5050_OLD],
		[HO5050_NEW],
		ISNULL([HO5050_NEW], 0) - ISNULL([HO5050_OLD], 0) AS [HO5050_I],

		-- HOO / NS (%)
		[HO5750_OLD],
		[HO5750_NEW],
		IIF(ISNULL([HO5750_OLD], 0) = ISNULL([HO5750_NEW], 0), 0, 
			ISNULL([TL2030_NEW] * [HO5750_NEW] - [HO5050_OLD], 0)) AS [HO5050_I__HO5750],  -- Increment on HOO coming from an impact on HOO / NS

		-- Total OVH
		[HO5090_OLD],
		[HO5090_NEW],
		ISNULL([HO5090_NEW], 0) - ISNULL([HO5090_OLD], 0) AS [HO5090_I],

		-- Total OVH / NS (%)
		[HO5790_OLD],
		[HO5790_NEW],
		IIF(ISNULL([HO5790_OLD], 0) = ISNULL([HO5790_NEW], 0), 0, 
			ISNULL([TL2030_NEW] * [HO5790_NEW] - [HO5090_OLD], 0)) AS [HO5090_I__HO5790],  -- Increment on Total OVH coming from an impact on Total OVH / NS

		-- Research & development costs (R&D)
		[RD6000_OLD],
		[RD6000_NEW],
		ISNULL([RD6000_NEW], 0) - ISNULL([RD6000_OLD], 0) AS [RD6000_I],

		-- R&D / NS (%)
		[RD6700_OLD],
		[RD6700_NEW],
		IIF(ISNULL([RD6700_OLD], 0) = ISNULL([RD6700_NEW], 0), 0, 
			ISNULL([TL2030_NEW] * [RD6700_NEW] - [RD6000_OLD], 0)) AS [RD6000_I__RD6700],  -- Increment on R&D coming from an impact on R&D / NS

		-- Other Income and Expenses (OIE)
		[IE7000_OLD],
		[IE7000_NEW],
		ISNULL([IE7000_NEW], 0) - ISNULL([IE7000_OLD], 0) AS [IE7000_I],

		-- OIE / NS (%)
		[IE7700_OLD],
		[IE7700_NEW],
		IIF(ISNULL([IE7700_OLD], 0) = ISNULL([IE7700_NEW], 0), 0, 
			ISNULL([TL2030_NEW] * [IE7700_NEW] - [IE7000_OLD], 0)) AS [IE7000_I__IE7700],  -- Increment on OIE coming from an impact on OIE / NS

		-- ROP (Trading Operating Income)
		[OI9000_OLD],
		[OI9000_NEW],
		ISNULL([OI9000_NEW], 0) - ISNULL([OI9000_OLD], 0) AS [OI9000_I],

		-- ROS (%)
		[OI9700_OLD],
		[OI9700_NEW],
		IIF(ISNULL([OI9700_OLD], 0) = ISNULL([OI9700_NEW], 0), 0, 
			ISNULL([TL2030_NEW] * [OI9700_NEW] - [OI9000_OLD], 0)) AS [OI9000_I__OI9700],  -- Increment on ROP coming from an impact on ROS

		/* Identify which KPIs were impacted (as a comma-separated list of values) */
		CONCAT_WS(','
			, CASE WHEN ISNULL([VL1000_OLD], 0) <> ISNULL([VL1000_NEW], 0) THEN 'VOL' END
			, CASE WHEN ISNULL([TL2030_OLD], 0) <> ISNULL([TL2030_NEW], 0) THEN 'NS' END
			, CASE WHEN ISNULL([TL2930_OLD], 0) <> ISNULL([TL2930_NEW], 0) THEN 'NS_BY_VOL' END
			, CASE WHEN ISNULL([CG3001_OLD], 0) <> ISNULL([CG3001_NEW], 0) THEN 'MAT_COS' END
			, CASE WHEN ISNULL([CG3901_OLD], 0) <> ISNULL([CG3901_NEW], 0) THEN 'MAT_COS_BY_VOL' END
			, CASE WHEN ISNULL([CG3002_OLD], 0) <> ISNULL([CG3002_NEW], 0) THEN 'MAT_OTH' END
			, CASE WHEN ISNULL([CG3902_OLD], 0) <> ISNULL([CG3902_NEW], 0) THEN 'MAT_OTH_BY_VOL' END
			, CASE WHEN ISNULL([CG3000_OLD], 0) <> ISNULL([CG3000_NEW], 0) THEN 'MAT' END
			, CASE WHEN ISNULL([CG3900_OLD], 0) <> ISNULL([CG3900_NEW], 0) THEN 'MAT_BY_VOL' END
			, CASE WHEN ISNULL([CG3011_OLD], 0) <> ISNULL([CG3011_NEW], 0) THEN 'MANUF_COS' END
			, CASE WHEN ISNULL([CG3911_OLD], 0) <> ISNULL([CG3911_NEW], 0) THEN 'MANUF_COS_BY_VOL' END
			, CASE WHEN ISNULL([CG3012_OLD], 0) <> ISNULL([CG3012_NEW], 0) THEN 'MANUF_OTH' END
			, CASE WHEN ISNULL([CG3912_OLD], 0) <> ISNULL([CG3912_NEW], 0) THEN 'MANUF_OTH_BY_VOL' END
			, CASE WHEN ISNULL([CG3010_OLD], 0) <> ISNULL([CG3010_NEW], 0) THEN 'MANUF' END
			, CASE WHEN ISNULL([CG3910_OLD], 0) <> ISNULL([CG3910_NEW], 0) THEN 'MANUF_BY_VOL' END
			, CASE WHEN ISNULL([CG3021_OLD], 0) <> ISNULL([CG3021_NEW], 0) THEN 'LOG_FTC_IFO' END
			, CASE WHEN ISNULL([CG3921_OLD], 0) <> ISNULL([CG3921_NEW], 0) THEN 'LOG_FTC_IFO_BY_VOL' END
			, CASE WHEN ISNULL([CG3022_OLD], 0) <> ISNULL([CG3022_NEW], 0) THEN 'LOG_USL' END
			, CASE WHEN ISNULL([CG3922_OLD], 0) <> ISNULL([CG3922_NEW], 0) THEN 'LOG_USL_BY_VOL' END
			, CASE WHEN ISNULL([CG3023_OLD], 0) <> ISNULL([CG3023_NEW], 0) THEN 'LOG_OTH' END
			, CASE WHEN ISNULL([CG3923_OLD], 0) <> ISNULL([CG3923_NEW], 0) THEN 'LOG_OTH_BY_VOL' END
			, CASE WHEN ISNULL([CG3020_OLD], 0) <> ISNULL([CG3020_NEW], 0) THEN 'LOG' END
			, CASE WHEN ISNULL([CG3920_OLD], 0) <> ISNULL([CG3920_NEW], 0) THEN 'LOG_BY_VOL' END
			, CASE WHEN ISNULL([CG3030_OLD], 0) <> ISNULL([CG3030_NEW], 0) THEN 'COGS' END
			, CASE WHEN ISNULL([CG3930_OLD], 0) <> ISNULL([CG3930_NEW], 0) THEN 'COGS_BY_VOL' END
			, CASE WHEN ISNULL([CG3040_OLD], 0) <> ISNULL([CG3040_NEW], 0) THEN 'GP' END
			, CASE WHEN ISNULL([CG3740_OLD], 0) <> ISNULL([CG3740_NEW], 0) THEN 'GP_BY_NS' END
			, CASE WHEN ISNULL([CG3940_OLD], 0) <> ISNULL([CG3940_NEW], 0) THEN 'GP_BY_VOL' END
			, CASE WHEN ISNULL([AP4001_OLD], 0) <> ISNULL([AP4001_NEW], 0) THEN 'AP_WRK' END
			, CASE WHEN ISNULL([AP4501_OLD], 0) <> ISNULL([AP4501_NEW], 0) THEN 'AP_WRK_BY_AP' END
			, CASE WHEN ISNULL([AP4002_OLD], 0) <> ISNULL([AP4002_NEW], 0) THEN 'AP_NON_WRK' END
			, CASE WHEN ISNULL([AP4003_OLD], 0) <> ISNULL([AP4003_NEW], 0) THEN 'AP_OTH' END
			, CASE WHEN ISNULL([AP4000_OLD], 0) <> ISNULL([AP4000_NEW], 0) THEN 'AP' END
			, CASE WHEN ISNULL([AP4700_OLD], 0) <> ISNULL([AP4700_NEW], 0) THEN 'AP_BY_NS' END
			, CASE WHEN ISNULL([AP4010_OLD], 0) <> ISNULL([AP4010_NEW], 0) THEN 'PM' END
			, CASE WHEN ISNULL([AP4710_OLD], 0) <> ISNULL([AP4710_NEW], 0) THEN 'PM_BY_NS' END
			, CASE WHEN ISNULL([SF5000_OLD], 0) <> ISNULL([SF5000_NEW], 0) THEN 'SF' END
			, CASE WHEN ISNULL([SF5700_OLD], 0) <> ISNULL([SF5700_NEW], 0) THEN 'SF_BY_NS' END
			, CASE WHEN ISNULL([SF5010_OLD], 0) <> ISNULL([SF5010_NEW], 0) THEN 'CM' END
			, CASE WHEN ISNULL([SF5710_OLD], 0) <> ISNULL([SF5710_NEW], 0) THEN 'CM_BY_NS' END
			, CASE WHEN ISNULL([HO5051_OLD], 0) <> ISNULL([HO5051_NEW], 0) THEN 'HOO_MKT' END
			, CASE WHEN ISNULL([HO5052_OLD], 0) <> ISNULL([HO5052_NEW], 0) THEN 'HOO_OPS' END
			, CASE WHEN ISNULL([HO5053_OLD], 0) <> ISNULL([HO5053_NEW], 0) THEN 'HOO_DBS' END
			, CASE WHEN ISNULL([HO5054_OLD], 0) <> ISNULL([HO5054_NEW], 0) THEN 'HOO_GLFUNC' END
			, CASE WHEN ISNULL([HO5050_OLD], 0) <> ISNULL([HO5050_NEW], 0) THEN 'HOO_TOT' END
			, CASE WHEN ISNULL([HO5750_OLD], 0) <> ISNULL([HO5750_NEW], 0) THEN 'HOO_TOT_BY_NS' END
			, CASE WHEN ISNULL([HO5090_OLD], 0) <> ISNULL([HO5090_NEW], 0) THEN 'OVH_TOT' END
			, CASE WHEN ISNULL([HO5790_OLD], 0) <> ISNULL([HO5790_NEW], 0) THEN 'OVH_TOT_BY_NS' END
			, CASE WHEN ISNULL([RD6000_OLD], 0) <> ISNULL([RD6000_NEW], 0) THEN 'RND' END
			, CASE WHEN ISNULL([RD6700_OLD], 0) <> ISNULL([RD6700_NEW], 0) THEN 'RND_BY_NS' END
			, CASE WHEN ISNULL([IE7000_OLD], 0) <> ISNULL([IE7000_NEW], 0) THEN 'OIE' END
			, CASE WHEN ISNULL([IE7700_OLD], 0) <> ISNULL([IE7700_NEW], 0) THEN 'OIE_BY_NS' END
			, CASE WHEN ISNULL([OI9000_OLD], 0) <> ISNULL([OI9000_NEW], 0) THEN 'ROP' END
			, CASE WHEN ISNULL([OI9700_OLD], 0) <> ISNULL([OI9700_NEW], 0) THEN 'ROP_BY_NS' END
		) AS TGT_IND_CSV
		INTO #tmp_UpdatedRows
	FROM OPENJSON(@JSONText, '$."UpdatedRows"') WITH (
		[ID]               bigint        '$."Updated"."ID"',
		[SCE_ELM_KEY]      varchar(64)   '$."Updated"."SCE_ELM_KEY"',
		[PER_ELM_COD]      varchar(30)   '$."Updated"."PER_ELM_COD"',
		[PER_OPN_FLG]      decimal(2,0)  '$."Updated"."PER_OPN_FLG"',
		[ETI_ELM_KEY]      varchar(64)   '$."Updated"."ETI_ELM_KEY"',
		[CUS_ELM_KEY]      varchar(64)   '$."Updated"."CUS_ELM_KEY"',
		[PDT_ELM_KEY]      varchar(64)   '$."Updated"."PDT_ELM_KEY"',
		[EIB_ELM_KEY]      varchar(64)   '$."Updated"."EIB_ELM_KEY"',
		[TTY_ELM_KEY]      varchar(64)   '$."Updated"."TTY_ELM_KEY"',
		[SAL_SUP_ELM_KEY]  varchar(64)   '$."Updated"."SAL_SUP_ELM_KEY"',
		[CAT_TYP_ELM_KEY]  varchar(64)   '$."Updated"."CAT_TYP_ELM_KEY"',

	-- Volume ---------------------------------------------------
		-- Volume sold  [VOL]
		[VL1000_OLD]  float  '$."Original"."VL1000_F"',
		[VL1000_NEW]  float  '$."Updated"."VL1000_F"',

	-- Topline --------------------------------------------------
		-- Net Sales (NS)  [NS]
		[TL2030_OLD]  float  '$."Original"."TL2030_F"',
		[TL2030_NEW]  float  '$."Updated"."TL2030_F"',
		[V_TL2030]    float  '$."Original".V_TL2030',  -- Variability (sensitivity to Volume sold)

		-- Net Sales / Volume sold  [NS_BY_VOL]
		[TL2930_OLD]  float  '$."Original"."TL2930_F"',
		[TL2930_NEW]  float  '$."Updated"."TL2930_F"',

	-- COGS -----------------------------------------------------
		-- Material Cost of Sales  [MAT_COS]
		[CG3001_OLD]  float  '$."Original"."CG3001_F"',
		[CG3001_NEW]  float  '$."Updated"."CG3001_F"',
		[V_CG3001]    float  '$."Original".V_CG3001',  -- Variability (sensitivity to Volume sold)

		-- Material Cost of Sales / Volume sold  [MAT_COS_BY_VOL]
		[CG3901_OLD]  float  '$."Original"."CG3901_F"',
		[CG3901_NEW]  float  '$."Updated"."CG3901_F"',

		-- Rest of Material Costs  [MAT_OTH]
		[CG3002_OLD]  float  '$."Original"."CG3002_F"',
		[CG3002_NEW]  float  '$."Updated"."CG3002_F"',
		[A_CG3002]    float  '$."Original".A_CG3002',  -- FCA
		[V_CG3002]    float  '$."Original".V_CG3002',  -- Variability (sensitivity to Volume sold)

		-- Rest of Material Costs / Volume sold  [MAT_OTH_BY_VOL]
		[CG3902_OLD]  float  '$."Original"."CG3902_F"',
		[CG3902_NEW]  float  '$."Updated"."CG3902_F"',

		-- Material Costs  [MAT]
		[CG3000_OLD]  float  '$."Original"."CG3000_F"',
		[CG3000_NEW]  float  '$."Updated"."CG3000_F"',

		-- Material Costs / Volume sold  [MAT_BY_VOL]
		[CG3900_OLD]  float  '$."Original"."CG3900_F"',
		[CG3900_NEW]  float  '$."Updated"."CG3900_F"',

		-- Manufacturing Cost of Sales  [MANUF_COS]
		[CG3011_OLD]  float  '$."Original"."CG3011_F"',
		[CG3011_NEW]  float  '$."Updated"."CG3011_F"',
		[V_CG3011]    float  '$."Original".V_CG3011',  -- Variability (sensitivity to Volume sold)

		-- Manufacturing Cost of Sales / Volume sold  [MANUF_COS_BY_VOL]
		[CG3911_OLD]  float  '$."Original"."CG3911_F"',
		[CG3911_NEW]  float  '$."Updated"."CG3911_F"',

		-- Rest of Manufacturing Costs  [MANUF_OTH]
		[CG3012_OLD]  float  '$."Original"."CG3012_F"',
		[CG3012_NEW]  float  '$."Updated"."CG3012_F"',
		[A_CG3012]    float  '$."Original".A_CG3012',  -- FCA
		[V_CG3012]    float  '$."Original".V_CG3012',  -- Variability (sensitivity to Volume sold)

		-- Rest of Manufacturing Costs / Volume sold  [MANUF_OTH_BY_VOL]
		[CG3912_OLD]  float  '$."Original"."CG3912_F"',
		[CG3912_NEW]  float  '$."Updated"."CG3912_F"',

		-- Manufacturing Costs  [MANUF]
		[CG3010_OLD]  float  '$."Original"."CG3010_F"',
		[CG3010_NEW]  float  '$."Updated"."CG3010_F"',

		-- Manufacturing Costs / Volume sold  [MANUF_BY_VOL]
		[CG3910_OLD]  float  '$."Original"."CG3910_F"',
		[CG3910_NEW]  float  '$."Updated"."CG3910_F"',

		-- Freight to Customers and Internal Freight Out  [LOG_FTC_IFO]
		[CG3021_OLD]  float  '$."Original"."CG3021_F"',
		[CG3021_NEW]  float  '$."Updated"."CG3021_F"',
		[V_CG3021]    float  '$."Original".V_CG3021',  -- Variability (sensitivity to Volume sold)

		-- Freight to Customers and Internal Freight Out / Volume sold  [LOG_FTC_IFO_BY_VOL]
		[CG3921_OLD]  float  '$."Original"."CG3921_F"',
		[CG3921_NEW]  float  '$."Updated"."CG3921_F"',

		-- Unsaleable  [LOG_USL]
		[CG3022_OLD]  float  '$."Original"."CG3022_F"',
		[CG3022_NEW]  float  '$."Updated"."CG3022_F"',
		[V_CG3022]    float  '$."Original".V_CG3022',  -- Variability (sensitivity to Volume sold)

		-- Unsaleable / Volume sold  [LOG_USL_BY_VOL]
		[CG3922_OLD]  float  '$."Original"."CG3922_F"',
		[CG3922_NEW]  float  '$."Updated"."CG3922_F"',

		-- Rest of Logistic Costs  [LOG_OTH]
		[CG3023_OLD]  float  '$."Original"."CG3023_F"',
		[CG3023_NEW]  float  '$."Updated"."CG3023_F"',
		[A_CG3023]    float  '$."Original".A_CG3023',  -- FCA
		[V_CG3023]    float  '$."Original".V_CG3023',  -- Variability (sensitivity to Volume sold)

		-- Rest of Logistic Costs / Volume sold  [LOG_OTH_BY_VOL]
		[CG3923_OLD]  float  '$."Original"."CG3923_F"',
		[CG3923_NEW]  float  '$."Updated"."CG3923_F"',

		-- Logistic Costs  [LOG]
		[CG3020_OLD]  float  '$."Original"."CG3020_F"',
		[CG3020_NEW]  float  '$."Updated"."CG3020_F"',

		-- Logistic Costs / Volume sold  [LOG_BY_VOL]
		[CG3920_OLD]  float  '$."Original"."CG3920_F"',
		[CG3920_NEW]  float  '$."Updated"."CG3920_F"',

		-- Total COGS (Cost of Goods Sold)  [COGS]
		[CG3030_OLD]  float  '$."Original"."CG3030_F"',
		[CG3030_NEW]  float  '$."Updated"."CG3030_F"',

		-- Cost of Goods Sold (COGS) / Volume sold  [COGS_BY_VOL]
		[CG3930_OLD]  float  '$."Original"."CG3930_F"',
		[CG3930_NEW]  float  '$."Updated"."CG3930_F"',

		-- Gross Profit  [GP]
		[CG3040_OLD]  float  '$."Original"."CG3040_F"',
		[CG3040_NEW]  float  '$."Updated"."CG3040_F"',

		-- Gross margin %  [GP_BY_NS]
		[CG3740_OLD]  float  '$."Original"."CG3740_F"',
		[CG3740_NEW]  float  '$."Updated"."CG3740_F"',

		-- GP / Vol  [GP_BY_VOL]
		[CG3940_OLD]  float  '$."Original"."CG3940_F"',
		[CG3940_NEW]  float  '$."Updated"."CG3940_F"',

	-- A&P ------------------------------------------------------
		-- A&P Working  [AP_WRK]
		[AP4001_OLD]  float  '$."Original"."AP4001_F"',
		[AP4001_NEW]  float  '$."Updated"."AP4001_F"',

		-- A&P Working / Total A&P (%)  [AP_WRK_BY_AP]
		[AP4501_OLD]  float  '$."Original"."AP4501_F"',
		[AP4501_NEW]  float  '$."Updated"."AP4501_F"',

		-- A&P Non Working  [AP_NON_WRK]
		[AP4002_OLD]  float  '$."Original"."AP4002_F"',
		[AP4002_NEW]  float  '$."Updated"."AP4002_F"',

		-- A&P Others  [AP_OTH]
		[AP4003_OLD]  float  '$."Original"."AP4003_F"',
		[AP4003_NEW]  float  '$."Updated"."AP4003_F"',

		-- Marketing Costs (A&P)  [AP]
		[AP4000_OLD]  float  '$."Original"."AP4000_F"',
		[AP4000_NEW]  float  '$."Updated"."AP4000_F"',

		-- A&P / NS (%)  [AP_BY_NS]
		[AP4700_OLD]  float  '$."Original"."AP4700_F"',
		[AP4700_NEW]  float  '$."Updated"."AP4700_F"',

		-- Product Margin (PM)  [PM]
		[AP4010_OLD]  float  '$."Original"."AP4010_F"',
		[AP4010_NEW]  float  '$."Updated"."AP4010_F"',

		-- PM / NS (%)  [PM_BY_NS]
		[AP4710_OLD]  float  '$."Original"."AP4710_F"',
		[AP4710_NEW]  float  '$."Updated"."AP4710_F"',

	-- Fixed Costs ----------------------------------------------
		-- Sales Force Costs (SF)  [SF]
		[SF5000_OLD]  float  '$."Original"."SF5000_F"',
		[SF5000_NEW]  float  '$."Updated"."SF5000_F"',

		-- SF / NS (%)  [SF_BY_NS]
		[SF5700_OLD]  float  '$."Original"."SF5700_F"',
		[SF5700_NEW]  float  '$."Updated"."SF5700_F"',

		-- Channel Margin (CM)  [CM]
		[SF5010_OLD]  float  '$."Original"."SF5010_F"',
		[SF5010_NEW]  float  '$."Updated"."SF5010_F"',

		-- CM / NS (%)  [CM_BY_NS]
		[SF5710_OLD]  float  '$."Original"."SF5710_F"',
		[SF5710_NEW]  float  '$."Updated"."SF5710_F"',

		-- HOO Market excluding OPS  [HOO_MKT]
		[HO5051_OLD]  float  '$."Original"."HO5051_F"',
		[HO5051_NEW]  float  '$."Updated"."HO5051_F"',

		-- HOO Operations  [HOO_OPS]
		[HO5052_OLD]  float  '$."Original"."HO5052_F"',
		[HO5052_NEW]  float  '$."Updated"."HO5052_F"',

		-- HOO DBS  [HOO_DBS]
		[HO5053_OLD]  float  '$."Original"."HO5053_F"',
		[HO5053_NEW]  float  '$."Updated"."HO5053_F"',

		-- HOO Global functions  [HOO_GLFUNC]
		[HO5054_OLD]  float  '$."Original"."HO5054_F"',
		[HO5054_NEW]  float  '$."Updated"."HO5054_F"',

		-- Head Office Overheads (HOO)  [HOO]
		[HO5050_OLD]  float  '$."Original"."HO5050_F"',
		[HO5050_NEW]  float  '$."Updated"."HO5050_F"',

		-- HOO / NS (%)  [HOO_TOT_BY_NS]
		[HO5750_OLD]  float  '$."Original"."HO5750_F"',
		[HO5750_NEW]  float  '$."Updated"."HO5750_F"',

		-- Total OVH  [OVH_TOT]
		[HO5090_OLD]  float  '$."Original"."HO5090_F"',
		[HO5090_NEW]  float  '$."Updated"."HO5090_F"',

		-- Total OVH / NS (%)  [OVH_TOT_BY_NS]
		[HO5790_OLD]  float  '$."Original"."HO5790_F"',
		[HO5790_NEW]  float  '$."Updated"."HO5790_F"',

		-- Research & development costs (R&D)  [RND]
		[RD6000_OLD]  float  '$."Original"."RD6000_F"',
		[RD6000_NEW]  float  '$."Updated"."RD6000_F"',

		-- R&D / NS (%)  [RND_BY_NS]
		[RD6700_OLD]  float  '$."Original"."RD6700_F"',
		[RD6700_NEW]  float  '$."Updated"."RD6700_F"',

		-- OIE ------------------------------------------------------
		-- Other Income and Expenses (OIE)  [OIE]
		[IE7000_OLD]  float  '$."Original"."IE7000_F"',
		[IE7000_NEW]  float  '$."Updated"."IE7000_F"',

		-- OIE / NS (%)  [OIE_BY_NS]
		[IE7700_OLD]  float  '$."Original"."IE7700_F"',
		[IE7700_NEW]  float  '$."Updated"."IE7700_F"',

		-- ROP ------------------------------------------------------
		-- ROP (Trading Operating Income)  [ROP]
		[OI9000_OLD]  float  '$."Original"."OI9000_F"',
		[OI9000_NEW]  float  '$."Updated"."OI9000_F"',

		-- ROS (%)  [ROP_BY_NS]
		[OI9700_OLD]  float  '$."Original"."OI9700_F"',
		[OI9700_NEW]  float  '$."Updated"."OI9700_F"'

	) u
	;
	
	/* Check the validity of the payload and raise an exception if any error is found */
	-- Input on a closed Period is not allowed
	IF (SELECT count(*) FROM #tmp_UpdatedRows WHERE [PER_OPN_FLG] = 0) > 0
	BEGIN
		INSERT INTO [flex].[t_flx_log] (run_id, process_name, event_type, event_payload) values (@RUN_ID, @PROCESS_NAME, 'error', json_object('error': 'WRITE_ON_CLOSED_PERIOD'));
		THROW @ERRNO__WRITE_ON_CLOSED_PERIOD
		    , N'<SQLError>You are trying to perform a writeback on a closed period, which might lead to inconsistent results. This feature is currently not supported.</SQLError>'
			, 0;
	END

	/* Log Update information */
	INSERT INTO [FLEX].[T_FLX_LOG] (RUN_ID, PROCESS_NAME, EVENT_TYPE, EVENT_PAYLOAD)
		SELECT
			@RUN_ID,
			@PROCESS_NAME,
			'parse_data' AS EVENT_TYPE,
			json_arrayagg(BY_IMPACTED_KPI)
		FROM (
			SELECT json_object(
				'scenario': [SCE_ELM_KEY],
				'impact_on': [TGT_IND_CSV],
				'rows_to_update': count(*)
			) AS BY_IMPACTED_KPI
			FROM #tmp_UpdatedRows
			GROUP BY [SCE_ELM_KEY], [TGT_IND_CSV]
		) t;

    /* Deadlock Management on the table F_FLX_SCE_SIM*/
    SELECT @Is_Locked = COUNT(*)
    FROM    sys.dm_tran_locks L
            JOIN sys.partitions P ON P.hobt_id = L.resource_associated_entity_id
            JOIN sys.objects O ON O.object_id = P.object_id
    WHERE   resource_database_id = db_id()
    AND     O.Name = N'F_FLX_SCE_SIM';

    IF @Is_Locked > 0
	    -- Log the information of the deadlock (Table, transaction ID, Session ID)
        INSERT INTO [FLEX].[T_FLX_LOG_DEADLOCK] ([object_name], [run_id], [session_id])
        SELECT  O.Name               AS object_name
               ,TST.transaction_id   AS run_id
               ,L.request_session_id AS ession_id
        FROM    sys.dm_tran_locks L
                JOIN sys.partitions P ON P.hobt_id = L.resource_associated_entity_id
                JOIN sys.objects O ON O.object_id = P.object_id
                JOIN sys.dm_exec_sessions ES ON ES.session_id = L.request_session_id
                JOIN sys.dm_tran_session_transactions TST ON ES.session_id = TST.session_id
        WHERE   resource_database_id = db_id()
        AND     O.Name = N'F_FLX_SCE_SIM';

    SET @delay = '00:00:01'
    -- loop 3 times to wait the unlock. The delay increases 1, 2 and 4 seconds
	-- the loop end if the unlock occurs
	WHILE( @VAR_WHILE < 3 
           AND @Is_Locked > 0)
    BEGIN
        SET @VAR_WHILE += 1;

		INSERT INTO [flex].[t_flx_log] (run_id, process_name, event_type, event_payload) values (@RUN_ID, @PROCESS_NAME, 'deadlock', json_object('waitfor': @delay+' seconds'));
	    WAITFOR DELAY @delay;

        SELECT @Is_Locked = COUNT(*)
        FROM    sys.dm_tran_locks L
                JOIN sys.partitions P ON P.hobt_id = L.resource_associated_entity_id
                JOIN sys.objects O ON O.object_id = P.object_id
        WHERE   resource_database_id = db_id()
		AND     O.Name = N'F_FLX_SCE_SIM';

        SET @delay = '00:00:' + RIGHT(CONCAT('0', @VAR_WHILE * 2), 2);

    END

	-- An error message is send if the table is still locked
	IF @Is_Locked > 0
	BEGIN
		INSERT INTO [flex].[t_flx_log] (run_id, process_name, event_type, event_payload) values (@RUN_ID, @PROCESS_NAME, 'error', json_object('error': 'DEADLOCK'));
		THROW @ERRNO__DATABASE_DEADLOCK
		    , N'<SQLError>Your modifications can not be applied due to database deadlock. Please, retry in few seconds.</SQLError>'
			, 0;
	END

	/* Merge new impacts to the scenario */
	MERGE INTO [flex].[F_FLX_SCE_SIM] WITH (HOLDLOCK) AS cur
		USING #tmp_UpdatedRows AS impact ON (cur.[ID] = impact.[ID])
	WHEN MATCHED AND impact.[TGT_IND_CSV] <> '' THEN UPDATE SET
		-- Volume sold
		[VL1000_I] = cur.[VL1000_I]
		  + impact.[VL1000_I]  -- Direct impact
		,

		-- Net Sales (NS)
		[TL2030_I] = cur.[TL2030_I]
		  + impact.[TL2030_I]  -- Direct impact
		  + impact.[TL2030_I__TL2930]  -- Impact on NS / Vol
		  + ISNULL(impact.[VL1000_I] * ((impact.[TL2030_OLD] / NULLIF(impact.[VL1000_OLD], 0)) * impact.[V_TL2030]), 0)  -- Increment coming from the sensitivity to Volume
		,

		-- Material Cost of Sales
		[CG3001_I] = cur.[CG3001_I]
		  + impact.[CG3001_I]  -- Direct impact
		  + impact.[CG3001_I__CG3901]  -- Impact on Material Cost of Sales / Vol
		  + ISNULL(impact.[VL1000_I] * ((impact.[CG3001_OLD] / NULLIF(impact.[VL1000_OLD], 0)) * impact.[V_CG3001]), 0)  -- Increment coming from the sensitivity to Volume
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[CG3001_OLD] / NULLIF(impact.[CG3000_OLD], 0)) * (impact.[CG3000_I] + impact.[CG3000_I__CG3900]), 0)  -- Split of the increment of Material Costs
		  + ISNULL((impact.[CG3001_OLD] / NULLIF(impact.[CG3030_OLD], 0)) * (impact.[CG3030_I] + impact.[CG3030_I__CG3930]), 0)  -- Split of the increment of COGS
		,

		-- Rest of Material Costs
		[CG3002_I] = cur.[CG3002_I]
		  + impact.[CG3002_I]  -- Direct impact
		  + impact.[CG3002_I__CG3902]  -- Impact on Rest of Material Costs / Vol
		  + ISNULL(impact.[VL1000_I] * ((impact.[CG3002_OLD] / NULLIF(impact.[VL1000_OLD], 0)) * impact.[V_CG3002] + impact.[A_CG3002]), 0)  -- Increment coming from the sensitivity to Volume
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[CG3002_OLD] / NULLIF(impact.[CG3000_OLD], 0)) * (impact.[CG3000_I] + impact.[CG3000_I__CG3900]), 0)  -- Split of the increment of Material Costs
		  + ISNULL((impact.[CG3002_OLD] / NULLIF(impact.[CG3030_OLD], 0)) * (impact.[CG3030_I] + impact.[CG3030_I__CG3930]), 0)  -- Split of the increment of COGS
		,

		-- Manufacturing Cost of Sales
		[CG3011_I] = cur.[CG3011_I]
		  + impact.[CG3011_I]  -- Direct impact
		  + impact.[CG3011_I__CG3911]  -- Impact on Manuf. Cost of Sales / Vol
		  + ISNULL(impact.[VL1000_I] * ((impact.[CG3011_OLD] / NULLIF(impact.[VL1000_OLD], 0)) * impact.[V_CG3011]), 0)  -- Increment coming from the sensitivity to Volume
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[CG3011_OLD] / NULLIF(impact.[CG3010_OLD], 0)) * (impact.[CG3010_I] + impact.[CG3010_I__CG3910]), 0)  -- Split of the increment of Manufacturing Costs
		  + ISNULL((impact.[CG3011_OLD] / NULLIF(impact.[CG3030_OLD], 0)) * (impact.[CG3030_I] + impact.[CG3030_I__CG3930]), 0)  -- Split of the increment of COGS
		,

		-- Rest of Manufacturing Costs
		[CG3012_I] = cur.[CG3012_I]
		  + impact.[CG3012_I]  -- Direct impact
		  + impact.[CG3012_I__CG3912]  -- Impact on Rest of Manuf. Costs / Vol
		  + ISNULL(impact.[VL1000_I] * ((impact.[CG3012_OLD] / NULLIF(impact.[VL1000_OLD], 0)) * impact.[V_CG3012] + impact.[A_CG3012]), 0)  -- Increment coming from the sensitivity to Volume
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[CG3012_OLD] / NULLIF(impact.[CG3010_OLD], 0)) * (impact.[CG3010_I] + impact.[CG3010_I__CG3910]), 0)  -- Split of the increment of Manufacturing Costs
		  + ISNULL((impact.[CG3012_OLD] / NULLIF(impact.[CG3030_OLD], 0)) * (impact.[CG3030_I] + impact.[CG3030_I__CG3930]), 0)  -- Split of the increment of COGS
		,

		-- Freight to Customers and Internal Freight Out
		[CG3021_I] = cur.[CG3021_I]
		  + impact.[CG3021_I]  -- Direct impact
		  + impact.[CG3021_I__CG3921]  -- Impact on FTC & IFO / Vol
		  + ISNULL(impact.[VL1000_I] * ((impact.[CG3021_OLD] / NULLIF(impact.[VL1000_OLD], 0)) * impact.[V_CG3021]), 0)  -- Increment coming from the sensitivity to Volume
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[CG3021_OLD] / NULLIF(impact.[CG3020_OLD], 0)) * (impact.[CG3020_I] + impact.[CG3020_I__CG3920]), 0)  -- Split of the increment of Logistic Costs
		  + ISNULL((impact.[CG3021_OLD] / NULLIF(impact.[CG3030_OLD], 0)) * (impact.[CG3030_I] + impact.[CG3030_I__CG3930]), 0)  -- Split of the increment of COGS
		,

		-- Unsaleable
		[CG3022_I] = cur.[CG3022_I]
		  + impact.[CG3022_I]  -- Direct impact
		  + impact.[CG3022_I__CG3922]  -- Impact on Unsaleable / Vol
		  + ISNULL(impact.[VL1000_I] * ((impact.[CG3022_OLD] / NULLIF(impact.[VL1000_OLD], 0)) * impact.[V_CG3022]), 0)  -- Increment coming from the sensitivity to Volume
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[CG3022_OLD] / NULLIF(impact.[CG3020_OLD], 0)) * (impact.[CG3020_I] + impact.[CG3020_I__CG3920]), 0)  -- Split of the increment of Logistic Costs
		  + ISNULL((impact.[CG3022_OLD] / NULLIF(impact.[CG3030_OLD], 0)) * (impact.[CG3030_I] + impact.[CG3030_I__CG3930]), 0)  -- Split of the increment of COGS
		,

		-- Rest of Logistic Costs
		[CG3023_I] = cur.[CG3023_I]
		  + impact.[CG3023_I]  -- Direct impact
		  + impact.[CG3023_I__CG3923]  -- Impact on Rest of Log. Costs / Vol
		  + ISNULL(impact.[VL1000_I] * ((impact.[CG3023_OLD] / NULLIF(impact.[VL1000_OLD], 0)) * impact.[V_CG3023] + impact.[A_CG3023]), 0)  -- Increment coming from the sensitivity to Volume
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[CG3023_OLD] / NULLIF(impact.[CG3020_OLD], 0)) * (impact.[CG3020_I] + impact.[CG3020_I__CG3920]), 0)  -- Split of the increment of Logistic Costs
		  + ISNULL((impact.[CG3023_OLD] / NULLIF(impact.[CG3030_OLD], 0)) * (impact.[CG3030_I] + impact.[CG3030_I__CG3930]), 0)  -- Split of the increment of COGS
		,

		-- A&P Working
		[AP4001_I] = cur.[AP4001_I]
		  + impact.[AP4001_I]  -- Direct impact
		  + impact.[AP4001_I__AP4501]  -- Impact on A&P Working / A&P
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[AP4001_OLD] / NULLIF(impact.[AP4000_OLD], 0)) * (impact.[AP4000_I] + impact.[AP4000_I__AP4700]), 0)  -- Split of the increment of A&P
		,

		-- A&P Non Working
		[AP4002_I] = cur.[AP4002_I]
		  + impact.[AP4002_I]  -- Direct impact
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[AP4002_OLD] / NULLIF(impact.[AP4000_OLD], 0)) * (impact.[AP4000_I] + impact.[AP4000_I__AP4700]), 0)  -- Split of the increment of A&P
		,

		-- A&P Others
		[AP4003_I] = cur.[AP4003_I]
		  + impact.[AP4003_I]  -- Direct impact
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[AP4003_OLD] / NULLIF(impact.[AP4000_OLD], 0)) * (impact.[AP4000_I] + impact.[AP4000_I__AP4700]), 0)  -- Split of the increment of A&P
		,

		-- Sales Force Costs (SF)
		[SF5000_I] = cur.[SF5000_I]
		  + impact.[SF5000_I]  -- Direct impact
		  + impact.[SF5000_I__SF5700]  -- Impact on SFC / NS
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[SF5000_OLD] / NULLIF(impact.[HO5090_OLD], 0)) * (impact.[HO5090_I] + impact.[HO5090_I__HO5790]), 0)  -- Split of the increment of Total OVH
		,

		-- HOO Market excluding OPS
		[HO5051_I] = cur.[HO5051_I]
		  + impact.[HO5051_I]  -- Direct impact
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[HO5051_OLD] / NULLIF(impact.[HO5050_OLD], 0)) * (impact.[HO5050_I] + impact.[HO5050_I__HO5750]), 0)  -- Split of the increment of HOO
		  + ISNULL((impact.[HO5051_OLD] / NULLIF(impact.[HO5090_OLD], 0)) * (impact.[HO5090_I] + impact.[HO5090_I__HO5790]), 0)  -- Split of the increment of Total OVH
		,

		-- HOO Operations
		[HO5052_I] = cur.[HO5052_I]
		  + impact.[HO5052_I]  -- Direct impact
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[HO5052_OLD] / NULLIF(impact.[HO5050_OLD], 0)) * (impact.[HO5050_I] + impact.[HO5050_I__HO5750]), 0)  -- Split of the increment of HOO
		  + ISNULL((impact.[HO5052_OLD] / NULLIF(impact.[HO5090_OLD], 0)) * (impact.[HO5090_I] + impact.[HO5090_I__HO5790]), 0)  -- Split of the increment of Total OVH
		,

		-- HOO DBS
		[HO5053_I] = cur.[HO5053_I]
		  + impact.[HO5053_I]  -- Direct impact
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[HO5053_OLD] / NULLIF(impact.[HO5050_OLD], 0)) * (impact.[HO5050_I] + impact.[HO5050_I__HO5750]), 0)  -- Split of the increment of HOO
		  + ISNULL((impact.[HO5053_OLD] / NULLIF(impact.[HO5090_OLD], 0)) * (impact.[HO5090_I] + impact.[HO5090_I__HO5790]), 0)  -- Split of the increment of Total OVH
		,

		-- HOO Global functions
		[HO5054_I] = cur.[HO5054_I]
		  + impact.[HO5054_I]  -- Direct impact
		  -- Split of the impact on higher levels in the hierarchy down to the Base KPIs
		  + ISNULL((impact.[HO5054_OLD] / NULLIF(impact.[HO5050_OLD], 0)) * (impact.[HO5050_I] + impact.[HO5050_I__HO5750]), 0)  -- Split of the increment of HOO
		  + ISNULL((impact.[HO5054_OLD] / NULLIF(impact.[HO5090_OLD], 0)) * (impact.[HO5090_I] + impact.[HO5090_I__HO5790]), 0)  -- Split of the increment of Total OVH
		,

		-- Research & development costs (R&D)
		[RD6000_I] = cur.[RD6000_I]
		  + impact.[RD6000_I]  -- Direct impact
		  + impact.[RD6000_I__RD6700]  -- Impact on R&D / NS
		,

		-- Other Income and Expenses (OIE)
		[IE7000_I] = cur.[IE7000_I]
		  + impact.[IE7000_I]  -- Direct impact
		  + impact.[IE7000_I__IE7700]  -- Impact on OIE / NS
		,

		[T_REC_UPD_TST] = getdate()
	
	OUTPUT impact.[SCE_ELM_KEY], impact.[TGT_IND_CSV], $action INTO @MERGE_LOG;

	/* Log the number of rows actually impacted */
	INSERT INTO [FLEX].[T_FLX_LOG] (RUN_ID, PROCESS_NAME, EVENT_TYPE, EVENT_PAYLOAD)
		SELECT
			@RUN_ID,
			@PROCESS_NAME,
			'save_impact' AS EVENT_TYPE,
			json_arrayagg(BY_IMPACTED_KPI)
		FROM (
			SELECT json_object(
				'scenario': [SCE_ELM_KEY],
				'impact_on': [TGT_IND_CSV],
				'rows_updated': count(*)
			) AS BY_IMPACTED_KPI
			FROM @MERGE_LOG
			GROUP BY [SCE_ELM_KEY], [TGT_IND_CSV]
		) t;

	/* Log detailed information */
	INSERT INTO [FLEX].[T_FLX_LOG] (RUN_ID, PROCESS_NAME, EVENT_TYPE, EVENT_PAYLOAD)
		SELECT
			@RUN_ID,
			@PROCESS_NAME,
			'debug' AS EVENT_TYPE,
			(
				SELECT
					ID,
					[SCE_ELM_KEY]     AS [Dimensions.SCE_ELM_KEY],
					[PER_ELM_COD]     AS [Dimensions.PER_ELM_COD],
					[PER_OPN_FLG]     AS [Dimensions.PER_OPN_FLG],
					[ETI_ELM_KEY]     AS [Dimensions.ETI_ELM_KEY],
					[CUS_ELM_KEY]     AS [Dimensions.CUS_ELM_KEY],
					[PDT_ELM_KEY]     AS [Dimensions.PDT_ELM_KEY],
					[EIB_ELM_KEY]     AS [Dimensions.EIB_ELM_KEY],
					[TTY_ELM_KEY]     AS [Dimensions.TTY_ELM_KEY],
					[SAL_SUP_ELM_KEY] AS [Dimensions.SAL_SUP_ELM_KEY],
					[CAT_TYP_ELM_KEY] AS [Dimensions.CAT_TYP_ELM_KEY],
					
					-- Volume sold
					[VL1000_OLD] AS [VL1000.OLD],
					[VL1000_NEW] AS [VL1000.NEW],
					[VL1000_I]   AS [VL1000.IMPACT.DIRECT],

					-- Net Sales (NS)
					[TL2030_OLD] AS [TL2030.OLD],
					[TL2030_NEW] AS [TL2030.NEW],
					[TL2030_I]   AS [TL2030.IMPACT.DIRECT],
					[TL2030_I__TL2930] AS [TL2030.IMPACT.TL2930],
					ISNULL([VL1000_I] * (([TL2030_OLD] / NULLIF([VL1000_OLD], 0)) * [V_TL2030]), 0) AS [TL2030.IMPACT.VL1000],
					[V_TL2030] AS [TL2030.VARIABILITY],

					[TL2930_OLD] AS [TL2930.OLD],
					[TL2930_NEW] AS [TL2930.NEW],
					
					-- Material Cost of Sales
					[CG3001_OLD] AS [CG3001.OLD],
					[CG3001_NEW] AS [CG3001.NEW],
					[CG3001_I]   AS [CG3001.IMPACT.DIRECT],
					[CG3001_I__CG3901] AS [CG3001.IMPACT.CG3901],
					ISNULL([VL1000_I] * (([CG3001_OLD] / NULLIF([VL1000_OLD], 0)) * [V_CG3001]), 0) AS [CG3001.IMPACT.VL1000],
					ISNULL(([CG3001_OLD] / NULLIF([CG3000_OLD], 0)) * [CG3000_I], 0)         AS [CG3001.IMPACT.CG3000],
					ISNULL(([CG3001_OLD] / NULLIF([CG3000_OLD], 0)) * [CG3000_I__CG3900], 0) AS [CG3001.IMPACT.CG3900],
					ISNULL(([CG3001_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I], 0)         AS [CG3001.IMPACT.CG3030],
					ISNULL(([CG3001_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I__CG3930], 0) AS [CG3001.IMPACT.CG3930],
					[V_CG3001] AS [CG3001.VARIABILITY],

					[CG3901_OLD] AS [CG3901.OLD],
					[CG3901_NEW] AS [CG3901.NEW],

					-- Rest of Material Costs
					[CG3002_OLD] AS [CG3002.OLD],
					[CG3002_NEW] AS [CG3002.NEW],
					[CG3002_I]   AS [CG3002.IMPACT.DIRECT],
					[CG3002_I__CG3902] AS [CG3002.IMPACT.CG3902],
					ISNULL([VL1000_I] * (([CG3002_OLD] / NULLIF([VL1000_OLD], 0)) * [V_CG3002] + [A_CG3002]), 0) AS [CG3002.IMPACT.VL1000],
					ISNULL(([CG3002_OLD] / NULLIF([CG3000_OLD], 0)) * [CG3000_I], 0)         AS [CG3002.IMPACT.CG3000],
					ISNULL(([CG3002_OLD] / NULLIF([CG3000_OLD], 0)) * [CG3000_I__CG3900], 0) AS [CG3002.IMPACT.CG3900],
					ISNULL(([CG3002_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I], 0)         AS [CG3002.IMPACT.CG3030],
					ISNULL(([CG3002_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I__CG3930], 0) AS [CG3002.IMPACT.CG3930],
					[V_CG3002] AS [CG3002.VARIABILITY],
					[A_CG3002] AS [CG3002.FCA],

					[CG3902_OLD] AS [CG3902.OLD],
					[CG3902_NEW] AS [CG3902.NEW],

					-- Material Costs
					[CG3000_OLD] AS [CG3000.OLD],
					[CG3000_NEW] AS [CG3000.NEW],
					[CG3000_I]   AS [CG3000.IMPACT.DIRECT],

					[CG3900_OLD] AS [CG3900.OLD],
					[CG3900_NEW] AS [CG3900.NEW],
					
					-- Manufacturing Cost of Sales
					[CG3011_OLD] AS [CG3011.OLD],
					[CG3011_NEW] AS [CG3011.NEW],
					[CG3011_I]   AS [CG3011.IMPACT.DIRECT],
					[CG3011_I__CG3911] AS [CG3011.IMPACT.CG3911],
					ISNULL([VL1000_I] * (([CG3011_OLD] / NULLIF([VL1000_OLD], 0)) * [V_CG3011]), 0) AS [CG3011.IMPACT.VL1000],
					ISNULL(([CG3011_OLD] / NULLIF([CG3010_OLD], 0)) * [CG3010_I], 0)         AS [CG3011.IMPACT.CG3010],
					ISNULL(([CG3011_OLD] / NULLIF([CG3010_OLD], 0)) * [CG3010_I__CG3910], 0) AS [CG3011.IMPACT.CG3910],
					ISNULL(([CG3011_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I], 0)         AS [CG3011.IMPACT.CG3030],
					ISNULL(([CG3011_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I__CG3930], 0) AS [CG3011.IMPACT.CG3930],
					[V_CG3011] AS [CG3011.VARIABILITY],

					[CG3911_OLD] AS [CG3911.OLD],
					[CG3911_NEW] AS [CG3911.NEW],

					-- Rest of Manufacturing Costs
					[CG3012_OLD] AS [CG3012.OLD],
					[CG3012_NEW] AS [CG3012.NEW],
					[CG3012_I]   AS [CG3012.IMPACT.DIRECT],
					[CG3012_I__CG3912] AS [CG3012.IMPACT.CG3912],
					ISNULL([VL1000_I] * (([CG3012_OLD] / NULLIF([VL1000_OLD], 0)) * [V_CG3012] + [A_CG3012]), 0) AS [CG3012.IMPACT.VL1000],
					ISNULL(([CG3012_OLD] / NULLIF([CG3010_OLD], 0)) * [CG3010_I], 0)         AS [CG3012.IMPACT.CG3010],
					ISNULL(([CG3012_OLD] / NULLIF([CG3010_OLD], 0)) * [CG3010_I__CG3910], 0) AS [CG3012.IMPACT.CG3910],
					ISNULL(([CG3012_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I], 0)         AS [CG3012.IMPACT.CG3030],
					ISNULL(([CG3012_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I__CG3930], 0) AS [CG3012.IMPACT.CG3930],
					[V_CG3012] AS [CG3012.VARIABILITY],
					[A_CG3012] AS [CG3012.FCA],
					
					[CG3912_OLD] AS [CG3912.OLD],
					[CG3912_NEW] AS [CG3912.NEW],

					-- Manufacturing Costs
					[CG3010_OLD] AS [CG3010.OLD],
					[CG3010_NEW] AS [CG3010.NEW],
					[CG3010_I]   AS [CG3010.IMPACT.NEW],

					[CG3910_OLD] AS [CG3910.OLD],
					[CG3910_NEW] AS [CG3910.NEW],

					-- Freight to Customers and Internal Freight Out
					[CG3021_OLD] AS [CG3021.OLD],
					[CG3021_NEW] AS [CG3021.NEW],
					[CG3021_I]   AS [CG3021.IMPACT.DIRECT],
					[CG3021_I__CG3921] AS [CG3021.IMPACT.CG3921],
					ISNULL([VL1000_I] * (([CG3021_OLD] / NULLIF([VL1000_OLD], 0)) * [V_CG3021]), 0) AS [CG3021.IMPACT.VL1000],
					ISNULL(([CG3021_OLD] / NULLIF([CG3020_OLD], 0)) * [CG3020_I], 0)         AS [CG3021.IMPACT.CG3020],
					ISNULL(([CG3021_OLD] / NULLIF([CG3020_OLD], 0)) * [CG3020_I__CG3920], 0) AS [CG3021.IMPACT.CG3920],
					ISNULL(([CG3021_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I], 0)         AS [CG3021.IMPACT.CG3030],
					ISNULL(([CG3021_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I__CG3930], 0) AS [CG3021.IMPACT.CG3930],
					[V_CG3021] AS [CG3021.VARIABILITY],

					[CG3921_OLD] AS [CG3921.OLD],
					[CG3921_NEW] AS [CG3921.NEW],
					
					-- Unsaleable
					[CG3022_OLD] AS [CG3022.OLD],
					[CG3022_NEW] AS [CG3022.NEW],
					[CG3022_I]   AS [CG3022.IMPACT.DIRECT],
					[CG3022_I__CG3922] AS [CG3022.IMPACT.CG3922],
					ISNULL([VL1000_I] * (([CG3022_OLD] / NULLIF([VL1000_OLD], 0)) * [V_CG3022]), 0) AS [CG3022.IMPACT.VL1000],
					ISNULL(([CG3022_OLD] / NULLIF([CG3020_OLD], 0)) * [CG3020_I], 0)         AS [CG3022.IMPACT.CG3020],
					ISNULL(([CG3022_OLD] / NULLIF([CG3020_OLD], 0)) * [CG3020_I__CG3920], 0) AS [CG3022.IMPACT.CG3920],
					ISNULL(([CG3022_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I], 0)         AS [CG3022.IMPACT.CG3030],
					ISNULL(([CG3022_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I__CG3930], 0) AS [CG3022.IMPACT.CG3930],
					[V_CG3022] AS [CG3022.VARIABILITY],

					[CG3922_OLD] AS [CG3922.OLD],
					[CG3922_NEW] AS [CG3922.NEW],
					
					-- Rest of Logistic Costs
					[CG3023_OLD] AS [CG3023.OLD],
					[CG3023_NEW] AS [CG3023.NEW],
					[CG3023_I]   AS [CG3023.IMPACT.DIRECT],
					[CG3023_I__CG3923] AS [CG3023.IMPACT.CG3923],
					ISNULL([VL1000_I] * (([CG3023_OLD] / NULLIF([VL1000_OLD], 0)) * [V_CG3023] + [A_CG3023]), 0) AS [CG3023.IMPACT.VL1000],
					ISNULL(([CG3023_OLD] / NULLIF([CG3020_OLD], 0)) * [CG3020_I], 0)         AS [CG3023.IMPACT.CG3020],
					ISNULL(([CG3023_OLD] / NULLIF([CG3020_OLD], 0)) * [CG3020_I__CG3920], 0) AS [CG3023.IMPACT.CG3920],
					ISNULL(([CG3023_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I], 0)         AS [CG3023.IMPACT.CG3030],
					ISNULL(([CG3023_OLD] / NULLIF([CG3030_OLD], 0)) * [CG3030_I__CG3930], 0) AS [CG3023.IMPACT.CG3930],
					[V_CG3023] AS [CG3023.VARIABILITY],
					[A_CG3023] AS [CG3023.FCA],
					
					[CG3923_OLD] AS [CG3923.OLD],
					[CG3923_NEW] AS [CG3923.NEW],
					
					-- Logistic Costs
					[CG3020_OLD] AS [CG3020.OLD],
					[CG3020_NEW] AS [CG3020.NEW],
					[CG3020_I]   AS [CG3020.IMPACT.DIRECT],

					[CG3920_OLD] AS [CG3920.OLD],
					[CG3920_NEW] AS [CG3920.NEW],

					-- Total COGS
					[CG3030_OLD] AS [CG3030.OLD],
					[CG3030_NEW] AS [CG3030.NEW],
					[CG3030_I]   AS [CG3030.IMPACT.DIRECT],

					[CG3930_OLD] AS [CG3930.OLD],
					[CG3930_NEW] AS [CG3930.NEW],

					-- A&P Working
					[AP4001_OLD] AS [AP4001.OLD],
					[AP4001_NEW] AS [AP4001.NEW],
					[AP4001_I]   AS [AP4001.IMPACT.DIRECT],
					[AP4001_I__AP4501] AS [AP4001.IMPACT.AP4501],
					ISNULL(([AP4001_OLD] / NULLIF([AP4000_OLD], 0)) * [AP4000_I], 0)         AS [AP4001.IMPACT.AP4000],
					ISNULL(([AP4001_OLD] / NULLIF([AP4000_OLD], 0)) * [AP4000_I__AP4700], 0) AS [AP4001.IMPACT.AP4700],

					[AP4501_OLD] AS [AP4501.OLD],
					[AP4501_NEW] AS [AP4501.NEW],

					-- A&P Non Working
					[AP4002_OLD] AS [AP4002.OLD],
					[AP4002_NEW] AS [AP4002.NEW],
					[AP4002_I]   AS [AP4002.IMPACT.DIRECT],
					ISNULL(([AP4002_OLD] / NULLIF([AP4000_OLD], 0)) * [AP4000_I], 0)         AS [AP4002.IMPACT.AP4000],
					ISNULL(([AP4002_OLD] / NULLIF([AP4000_OLD], 0)) * [AP4000_I__AP4700], 0) AS [AP4002.IMPACT.AP4700],
					
					-- A&P Others
					[AP4003_OLD] AS [AP4003.OLD],
					[AP4003_NEW] AS [AP4003.NEW],
					[AP4003_I]   AS [AP4003.IMPACT.DIRECT],
					ISNULL(([AP4003_OLD] / NULLIF([AP4000_OLD], 0)) * [AP4000_I], 0)         AS [AP4003.IMPACT.AP4000],
					ISNULL(([AP4003_OLD] / NULLIF([AP4000_OLD], 0)) * [AP4000_I__AP4700], 0) AS [AP4003.IMPACT.AP4700],
					
					-- Marketing Costs (A&P)
					[AP4000_OLD] AS [AP4000.OLD],
					[AP4000_NEW] AS [AP4000.NEW],
					[AP4000_I]   AS [AP4000.IMPACT.DIRECT],

					[AP4700_OLD] AS [AP4700.OLD],
					[AP4700_NEW] AS [AP4700.NEW],

					-- Sales Force Costs (SF)
					[SF5000_OLD] AS [SF5000.OLD],
					[SF5000_NEW] AS [SF5000.NEW],
					[SF5000_I]   AS [SF5000.IMPACT.DIRECT],
					[SF5000_I__SF5700] AS [SF5000.IMPACT.SF5700],
					ISNULL(([SF5000_OLD] / NULLIF([HO5090_OLD], 0)) * [HO5090_I], 0)         AS [SF5000.IMPACT.HO5090],
					ISNULL(([SF5000_OLD] / NULLIF([HO5090_OLD], 0)) * [HO5090_I__HO5790], 0) AS [SF5000.IMPACT.HO5790],
					
					[SF5700_OLD] AS [SF5700.OLD],
					[SF5700_NEW] AS [SF5700.NEW],
					
					-- HOO Market excluding OPS
					[HO5051_OLD] AS [HO5051.OLD],
					[HO5051_NEW] AS [HO5051.NEW],
					[HO5051_I]   AS [HO5051.IMPACT.DIRECT],
					ISNULL(([HO5051_OLD] / NULLIF([HO5050_OLD], 0)) * [HO5050_I], 0)         AS [HO5051.IMPACT.HO5050],
					ISNULL(([HO5051_OLD] / NULLIF([HO5050_OLD], 0)) * [HO5050_I__HO5750], 0) AS [HO5051.IMPACT.HO5750],
					ISNULL(([HO5051_OLD] / NULLIF([HO5090_OLD], 0)) * [HO5090_I], 0)         AS [HO5051.IMPACT.HO5090],
					ISNULL(([HO5051_OLD] / NULLIF([HO5090_OLD], 0)) * [HO5090_I__HO5790], 0) AS [HO5051.IMPACT.HO5790],

					-- HOO Operations
					[HO5052_OLD] AS [HO5052.OLD],
					[HO5052_NEW] AS [HO5052.NEW],
					[HO5052_I]   AS [HO5052.IMPACT.DIRECT],
					ISNULL(([HO5052_OLD] / NULLIF([HO5050_OLD], 0)) * [HO5050_I], 0)         AS [HO5052.IMPACT.HO5050],
					ISNULL(([HO5052_OLD] / NULLIF([HO5050_OLD], 0)) * [HO5050_I__HO5750], 0) AS [HO5052.IMPACT.HO5750],
					ISNULL(([HO5052_OLD] / NULLIF([HO5090_OLD], 0)) * [HO5090_I], 0)         AS [HO5052.IMPACT.HO5090],
					ISNULL(([HO5052_OLD] / NULLIF([HO5090_OLD], 0)) * [HO5090_I__HO5790], 0) AS [HO5052.IMPACT.HO5790],

					-- HOO DBS
					[HO5053_OLD] AS [HO5053.OLD],
					[HO5053_NEW] AS [HO5053.NEW],
					[HO5053_I]   AS [HO5053.IMPACT.DIRECT],
					ISNULL(([HO5053_OLD] / NULLIF([HO5050_OLD], 0)) * [HO5050_I], 0)         AS [HO5053.IMPACT.HO5050],
					ISNULL(([HO5053_OLD] / NULLIF([HO5050_OLD], 0)) * [HO5050_I__HO5750], 0) AS [HO5053.IMPACT.HO5750],
					ISNULL(([HO5053_OLD] / NULLIF([HO5090_OLD], 0)) * [HO5090_I], 0)         AS [HO5053.IMPACT.HO5090],
					ISNULL(([HO5053_OLD] / NULLIF([HO5090_OLD], 0)) * [HO5090_I__HO5790], 0) AS [HO5053.IMPACT.HO5790],

					-- HOO Global functions
					[HO5054_OLD] AS [HO5054.OLD],
					[HO5054_NEW] AS [HO5054.NEW],
					[HO5054_I]   AS [HO5054.IMPACT.DIRECT],
					ISNULL(([HO5054_OLD] / NULLIF([HO5050_OLD], 0)) * [HO5050_I], 0)         AS [HO5054.IMPACT.HO5050],
					ISNULL(([HO5054_OLD] / NULLIF([HO5050_OLD], 0)) * [HO5050_I__HO5750], 0) AS [HO5054.IMPACT.HO5750],
					ISNULL(([HO5054_OLD] / NULLIF([HO5090_OLD], 0)) * [HO5090_I], 0)         AS [HO5054.IMPACT.HO5090],
					ISNULL(([HO5054_OLD] / NULLIF([HO5090_OLD], 0)) * [HO5090_I__HO5790], 0) AS [HO5054.IMPACT.HO5790],
					
					-- Total HOO
					[HO5050_OLD] AS [HO5050.OLD],
					[HO5050_NEW] AS [HO5050.NEW],
					[HO5050_I]   AS [HO5050.IMPACT.DIRECT],

					[HO5750_OLD] AS [HO5750.OLD],
					[HO5750_NEW] AS [HO5750.NEW],

					-- Total OVH
					[HO5090_OLD] AS [HO5090.OLD],
					[HO5090_NEW] AS [HO5090.NEW],
					[HO5090_I]   AS [HO5090.IMPACT.DIRECT],

					[HO5790_OLD] AS [HO5790.OLD],
					[HO5790_NEW] AS [HO5790.NEW],

					-- Research & development costs (R&D)
					[RD6000_OLD] AS [RD6000.OLD],
					[RD6000_NEW] AS [RD6000.NEW],
					[RD6000_I]   AS [RD6000.IMPACT.DIRECT],
					[RD6000_I__RD6700] AS [RD6000.IMPACT.RD6700],
					
					[RD6700_OLD] AS [RD6700.OLD],
					[RD6700_NEW] AS [RD6700.NEW],
					
					-- Other Income and Expenses (OIE)
					[IE7000_OLD] AS [IE7000.OLD],
					[IE7000_NEW] AS [IE7000.NEW],
					[IE7000_I]   AS [IE7000.IMPACT.DIRECT],
					[IE7000_I__IE7700] AS [IE7000.IMPACT.IE7700],
					
					[IE7700_OLD] AS [IE7700.OLD],
					[IE7700_NEW] AS [IE7700.NEW]

				FROM #tmp_UpdatedRows
				WHERE [TGT_IND_CSV] <> ''
				FOR JSON PATH

			) AS EVENT_PAYLOAD;

	/* Log end of Simulation */
	INSERT INTO [FLEX].[T_FLX_LOG] (RUN_ID, PROCESS_NAME, EVENT_TYPE) VALUES (@RUN_ID, @PROCESS_NAME, 'end');

END;
GO

