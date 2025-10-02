/****** Object:  View [flex].[VW_FLX_SCE_SIM]    Script Date: 10/2/2025 9:17:48 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE   VIEW  [flex].[VW_FLX_SCE_SIM]
AS
SELECT	  [ID]                                     -- Unique ID of the Scenario tuple
		, [SCE_ELM_KEY]                            -- Scenario Key : concatenation of CBU and Scenario Code
		, [PER_ELM_COD]                            -- Period
		, ABS([PER_ACT_FLG]-1) AS [PER_OPN_FLG]    -- Indicates whether the Period is Open (i.e. can be written to)
		, [ETI_ELM_KEY]                            -- Entity key
		, [CUS_ELM_KEY]                            -- Customer key
		, [PDT_ELM_KEY]                            -- Product key
		, [EIB_ELM_KEY]                            -- EIB key
		, [TTY_ELM_KEY]                            -- Territory key
		, [SAL_SUP_ELM_KEY]                        -- SU/SP split key
		, [CAT_TYP_ELM_KEY]                        -- Managerial / Interco Margin / IFRS View key

-- Volume -----------------------------------
		-- Volume sold  [VOL]
		, [VL1000_B]                               -- Base (from scenario init)
		, ([VL1000_B] + [VL1000_I]) AS [VL1000_C]  -- Current (Base + Increment)
		, ([VL1000_B] + [VL1000_I]) AS [VL1000_F]  -- Flex (Base + Increment) - Technical column for Power ON writeback
		, [VL1000C1]                               -- Comparable scenario 1
		, [VL1000C2]                               -- Comparable scenario 2
		, [VL1000C3]                               -- Comparable scenario 3

-- Topline ----------------------------------
		-- Net Sales  [NS]
		, [TL2030_B]
		, ([TL2030_B] + [TL2030_I]) AS [TL2030_C]
		, ([TL2030_B] + [TL2030_I]) AS [TL2030_F]
		, [TL2030C1]
		, [TL2030C2]
		, [TL2030C3]
		, [V_TL2030]  -- Variability (sensitivity to Volume sold)

		, ([TL2030_B] +  [TL2030_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) AS [TL2930_F]  -- Net Sales / Volume  [NS_BY_VOL]

		-- Indirect impact from sensitivity to another indicator
		-- i.e. incremental Net Sales amount per incremental unit of Volume sold, coming from Variability
		, ([TL2030_B] +  [TL2030_I]) / NULLIF([VL1000_B] + [VL1000_I], 0)  -- Net Sales / Volume
		  * [V_TL2030]  -- Variability (sensitivity to Volume sold)
		  AS [F_TL2030]

-- COGS -------------------------------------
		-- Material Costs of Sales  [MAT_COS]
	 	, [CG3001_B]
		, ([CG3001_B] + [CG3001_I]) AS [CG3001_C]
		, ([CG3001_B] + [CG3001_I]) AS [CG3001_F]
		, [CG3001C1]
		, [CG3001C2]
		, [CG3001C3]
		, [V_CG3001]  -- Variability (sensitivity to Volume sold)
		
		, ([CG3001_B] +  [CG3001_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) AS [CG3901_F]  -- Material Cost of Sales / Volume  [MAT_COS_BY_VOL]

		-- Incremental Material Costs of Sales amount per incremental unit of Volume sold, coming from Variability
		, ([CG3001_B] +  [CG3001_I]) / NULLIF([VL1000_B] + [VL1000_I], 0)  -- Material Cost of Sales / Volume
		  * [V_CG3001]  -- Variability (sensitivity to Volume sold)
		  AS [F_CG3001]
		
		-- Rest of Material Costs  [MAT_OTH]
	 	, [CG3002_B]
		, ([CG3002_B] + [CG3002_I]) AS [CG3002_C]
		, ([CG3002_B] + [CG3002_I]) AS [CG3002_F]
		, [CG3002C1]
		, [CG3002C2]
		, [CG3002C3]
		, [A_CG3002]  -- FCA
		, [V_CG3002]  -- Variability (sensitivity to Volume sold)
		
		, ([CG3002_B] +  [CG3002_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) AS [CG3902_F]  -- Rest of Material Costs / Volume  [MAT_OTH_BY_VOL]
		
		-- Incremental Rest of Material Costs amount per incremental unit of Volume sold, coming from Variability and FCA
		, ([CG3002_B] +  [CG3002_I]) / NULLIF([VL1000_B] + [VL1000_I], 0)  -- Rest of Material Costs / Volume
		  * [V_CG3002]  -- Variability (sensitivity to Volume sold)
		  + [A_CG3002]  -- FCA
		  AS [F_CG3002]
		
		-- Material Costs  [MAT]
	 	, [CG3001_B] + [CG3002_B]                               AS [CG3000_B]
		, ([CG3001_B] + [CG3001_I]) + ([CG3002_B] + [CG3002_I]) AS [CG3000_C]
		, ([CG3001_B] + [CG3001_I]) + ([CG3002_B] + [CG3002_I]) AS [CG3000_F]
		, [CG3001C1] + [CG3002C1]                               AS [CG3000C1]
		, [CG3001C2] + [CG3002C2]                               AS [CG3000C2]
		, [CG3001C3] + [CG3002C3]                               AS [CG3000C3]
	 
		-- Incremental Material Costs amount per incremental unit of Volume sold, coming from its components
		, ([CG3001_B] +  [CG3001_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3001]                 -- [F_CG3001]
		  + ([CG3002_B] +  [CG3002_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3002] + [A_CG3002]  -- [F_CG3002]
		  AS [F_CG3000]
		
		-- Manufacturing Costs of Sales  [MANUF_COS]
		, [CG3011_B]
		, ([CG3011_B] + [CG3011_I]) AS [CG3011_C]
		, ([CG3011_B] + [CG3011_I]) AS [CG3011_F]
		, [CG3011C1]
		, [CG3011C2]
		, [CG3011C3]
		, [V_CG3011]  -- Variability (sensitivity to Volume sold)
		
		, ([CG3011_B] +  [CG3011_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) AS [CG3911_F]  -- Manufacturing Cost of Sales / Volume  [MANUF_COS_BY_VOL]
		
		-- Incremental Manufacturing Costs of Sales amount per incremental unit of Volume sold, coming from Variability
		, ([CG3011_B] +  [CG3011_I]) / NULLIF([VL1000_B] + [VL1000_I], 0)  -- Manufacturing Cost of Sales / Volume
		  * [V_CG3011]  -- Variability (sensitivity to Volume sold)
		  AS [F_CG3011]
		
		-- Rest of Manufacturing Costs  [MANUF_OTH]
		, [CG3012_B]
		, ([CG3012_B] + [CG3012_I]) AS [CG3012_C]
		, ([CG3012_B] + [CG3012_I]) AS [CG3012_F]
		, [CG3012C1]
		, [CG3012C2]
		, [CG3012C3]
		, [A_CG3012]  -- FCA
		, [V_CG3012]  -- Variability (sensitivity to Volume sold)
		
		, ([CG3012_B] +  [CG3012_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) AS [CG3912_F]  -- Rest of Manufacturing Costs / Volume  [MANUF_OTH_BY_VOL]
		
		-- Incremental Rest of Manufacturing Costs amount per incremental unit of Volume sold, coming from Variability and FCA
		, ([CG3012_B] +  [CG3012_I]) / NULLIF([VL1000_B] + [VL1000_I], 0)  -- Rest of Manufacturing Costs / Volume
		  * [V_CG3012]  -- Variability (sensitivity to Volume sold)
		  + [A_CG3012]  -- FCA
		  AS [F_CG3012]
		
		-- Manufacturing Costs  [MANUF]
		, [CG3011_B] + [CG3012_B]                               AS [CG3010_B]
		, ([CG3011_B] + [CG3011_I]) + ([CG3012_B] + [CG3012_I]) AS [CG3010_C]
		, ([CG3011_B] + [CG3011_I]) + ([CG3012_B] + [CG3012_I]) AS [CG3010_F]
		, [CG3011C1] + [CG3012C1]                               AS [CG3010C1]
		, [CG3011C2] + [CG3012C2]                               AS [CG3010C2]
		, [CG3011C3] + [CG3012C3]                               AS [CG3010C3]
		
		-- Incremental Manufacturing Costs amount per incremental unit of Volume sold, coming from its components
		, ([CG3011_B] +  [CG3011_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3011]                 -- [F_CG3011]
		  + ([CG3012_B] +  [CG3012_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3012] + [A_CG3012]  -- [F_CG3012]
		  AS [F_CG3010]
		
		-- Freight to Customers and Internal Freight Out  [LOG_FTC_IFO]
		, [CG3021_B]
		, ([CG3021_B] + [CG3021_I]) AS [CG3021_C]
		, ([CG3021_B] + [CG3021_I]) AS [CG3021_F]
		, [CG3021C1]
		, [CG3021C2]
		, [CG3021C3]
		, [V_CG3021]  -- Variability (sensitivity to Volume sold)
		
		, ([CG3021_B] +  [CG3021_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) AS [CG3921_F]  -- FTC & IFO / Volume  [LOG_FTC_IFO_BY_VOL]
		
		-- Incremental Freight to Customers and Internal Freight Out amount per incremental unit of Volume sold, coming from Variability
		, ([CG3021_B] +  [CG3021_I]) / NULLIF([VL1000_B] + [VL1000_I], 0)  -- FTC & IFO / Volume
		  * [V_CG3021]  -- Variability (sensitivity to Volume sold)
		  AS [F_CG3021]
		
		-- Unsaleable  [LOG_USL]
		, [CG3022_B]
		, ([CG3022_B] + [CG3022_I]) AS [CG3022_C]
		, ([CG3022_B] + [CG3022_I]) AS [CG3022_F]
		, [CG3022C1]
		, [CG3022C2]
		, [CG3022C3]
		, [V_CG3022]  -- Variability (sensitivity to Volume sold)
		
		, ([CG3022_B] +  [CG3022_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) AS [CG3922_F]  -- Unsaleable / Volume  [LOG_USL_BY_VOL]
		
		-- Incremental Unsaleable amount per incremental unit of Volume sold, coming from Variability
		, ([CG3022_B] +  [CG3022_I]) / NULLIF([VL1000_B] + [VL1000_I], 0)  -- Unsaleable / Volume
		  * [V_CG3022]  -- Variability (sensitivity to Volume sold)
		  AS [F_CG3022]
		
		-- Rest of Logistic Costs  [LOG_OTH]
		, [CG3023_B]
		, ([CG3023_B] + [CG3023_I]) AS [CG3023_C]
		, ([CG3023_B] + [CG3023_I]) AS [CG3023_F]
		, [CG3023C1]
		, [CG3023C2]
		, [CG3023C3]
		, [A_CG3023]  -- FCA
		, [V_CG3023]  -- Variability (sensitivity to Volume sold)

		, ([CG3023_B] + [CG3023_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) AS [CG3923_F]  -- Rest of Logistic Costs / Volume  [LOG_OTH_BY_VOL]
		
		-- Incremental Rest of Logistic Costs amount per incremental unit of Volume sold, coming from Variability and FCA
		, ([CG3023_B] +  [CG3023_I]) / NULLIF([VL1000_B] + [VL1000_I], 0)  -- Rest of Logistic Costs / Volume
		  * [V_CG3023]  -- Variability (sensitivity to Volume sold)
		  + [A_CG3023]  -- FCA
		  AS [F_CG3023]
		
		-- Logistic Costs  [LOG]
		, [CG3021_B] + [CG3022_B] + [CG3023_B]                                              AS [CG3020_B]
		, ([CG3021_B] + [CG3021_I]) + ([CG3022_B] + [CG3022_I]) + ([CG3023_B] + [CG3023_I]) AS [CG3020_C]
		, ([CG3021_B] + [CG3021_I]) + ([CG3022_B] + [CG3022_I]) + ([CG3023_B] + [CG3023_I]) AS [CG3020_F]
		, [CG3021C1] + [CG3022C1] + [CG3023C1]                                              AS [CG3020C1]
		, [CG3021C2] + [CG3022C2] + [CG3023C2]                                              AS [CG3020C2]
		, [CG3021C3] + [CG3022C3] + [CG3023C3]                                              AS [CG3020C3]
		
		-- Incremental Logistic Costs amount per incremental unit of Volume sold, coming from its components
		, ([CG3021_B] +  [CG3021_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3021]                 -- [F_CG3021]
		  + ([CG3022_B] +  [CG3022_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3022]               -- [F_CG3022]
		  + ([CG3023_B] +  [CG3023_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3023] + [A_CG3023]  -- [F_CG3023]
		  AS [F_CG3020]
		
		-- Total COGS  [COGS]
		, [CG3001_B] + [CG3002_B]
		  + [CG3011_B] + [CG3012_B] 
		  + [CG3021_B] + [CG3022_B] + [CG3023_B] AS [CG3030_B]
		, ([CG3001_B] + [CG3001_I]) + ([CG3002_B] + [CG3002_I])
		  + ([CG3011_B] + [CG3011_I]) + ([CG3012_B] + [CG3012_I])
		  + ([CG3021_B] + [CG3021_I]) + ([CG3022_B] + [CG3022_I]) + ([CG3023_B] + [CG3023_I]) AS [CG3030_C]
		, ([CG3001_B] + [CG3001_I]) + ([CG3002_B] + [CG3002_I])
		  + ([CG3011_B] + [CG3011_I]) + ([CG3012_B] + [CG3012_I])
		  + ([CG3021_B] + [CG3021_I]) + ([CG3022_B] + [CG3022_I]) + ([CG3023_B] + [CG3023_I]) AS [CG3030_F]
		, [CG3001C1] + [CG3002C1]
		  + [CG3011C1] + [CG3012C1] 
		  + [CG3021C1] + [CG3022C1] + [CG3023C1] AS [CG3030C1]
		, [CG3001C2] + [CG3002C2]
		  + [CG3011C2] + [CG3012C2] 
		  + [CG3021C2] + [CG3022C2] + [CG3023C2] AS [CG3030C2]
		, [CG3001C3] + [CG3002C3]
		  + [CG3011C3] + [CG3012C3] 
		  + [CG3021C3] + [CG3022C3] + [CG3023C3] AS [CG3030C3]

		-- Incremental COGS amount per incremental unit of Volume sold, coming from its components
		-- from Material Costs
		, ([CG3001_B] +  [CG3001_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3001]                 -- [F_CG3001]
		  + ([CG3002_B] +  [CG3002_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3002] + [A_CG3002]  -- [F_CG3002]
		-- from Manufacturing Costs
		  + ([CG3011_B] +  [CG3011_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3011]               -- [F_CG3011]
		  + ([CG3012_B] +  [CG3012_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3012] + [A_CG3012]  -- [F_CG3012]
		-- from Logistic Costs
		  + ([CG3021_B] +  [CG3021_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3021]               -- [F_CG3021]
		  + ([CG3022_B] +  [CG3022_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3022]               -- [F_CG3022]
		  + ([CG3023_B] +  [CG3023_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3023] + [A_CG3023]  -- [F_CG3023]
		  AS [F_CG3030]

/* BEGIN: To be removed from PBI then from this view */
		-- Incremental COGS amount per incremental unit of Volume sold, coming from its components
		-- from Material Costs
		, ([CG3001_B] +  [CG3001_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3001]                 -- [F_CG3001]
		  + ([CG3002_B] +  [CG3002_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3002] + [A_CG3002]  -- [F_CG3002]
		-- from Manufacturing Costs
		  + ([CG3011_B] +  [CG3011_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3011]               -- [F_CG3011]
		  + ([CG3012_B] +  [CG3012_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3012] + [A_CG3012]  -- [F_CG3012]
		-- from Logistic Costs
		  + ([CG3021_B] +  [CG3021_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3021]               -- [F_CG3021]
		  + ([CG3022_B] +  [CG3022_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3022]               -- [F_CG3022]
		  + ([CG3023_B] +  [CG3023_I]) / NULLIF([VL1000_B] + [VL1000_I], 0) * [V_CG3023] + [A_CG3023]  -- [F_CG3023]
		  AS [F_CG3930]
/* END */

		-- Total COGS / Volume  [COGS_BY_VOL]
		, (([CG3001_B] + [CG3001_I]) + ([CG3002_B] + [CG3002_I])
		  + ([CG3011_B] + [CG3011_I]) + ([CG3012_B] + [CG3012_I])
		  + ([CG3021_B] + [CG3021_I]) + ([CG3022_B] + [CG3022_I]) + ([CG3023_B] + [CG3023_I]))
		  / NULLIF([VL1000_B] + [VL1000_I], 0) AS [CG3930_F]

-- A&P --------------------------------------
		-- A&P Working  [AP_WRK]
		, [AP4001_B]
		, ([AP4001_B] + [AP4001_I]) AS [AP4001_C]
		, ([AP4001_B] + [AP4001_I]) AS [AP4001_F]
		, [AP4001C1]
		, [AP4001C2]
		, [AP4001C3]

		-- A&P Non Working  [AP_NON_WRK]
		, [AP4002_B]
		, ([AP4002_B] + [AP4002_I]) AS [AP4002_C]
		, ([AP4002_B] + [AP4002_I]) AS [AP4002_F]
		, [AP4002C1]
		, [AP4002C2]
		, [AP4002C3]

		-- A&P Other  [AP_OTH]
		, [AP4003_B]
		, ([AP4003_B] + [AP4003_I]) AS [AP4003_C]
		, ([AP4003_B] + [AP4003_I]) AS [AP4003_F]
		, [AP4003C1]
		, [AP4003C2]
		, [AP4003C3]

		-- Marketing Costs (A&P)  [AP]
		, [AP4001_B] + [AP4002_B] + COALESCE([AP4003_B],0) AS [AP4000_B]
		, ([AP4001_B] + [AP4001_I]) + ([AP4002_B] + [AP4002_I]) + COALESCE(([AP4003_B] + [AP4003_I]),0) AS [AP4000_C]
		, ([AP4001_B] + [AP4001_I]) + ([AP4002_B] + [AP4002_I]) + COALESCE(([AP4003_B] + [AP4003_I]),0) AS [AP4000_F]
		, [AP4001C1] + [AP4002C1] + COALESCE([AP4003C1],0) AS [AP4000C1]
		, [AP4001C2] + [AP4002C2] + COALESCE([AP4003C2],0) AS [AP4000C2]
		, [AP4001C3] + [AP4002C3] + COALESCE([AP4003C3],0) AS [AP4000C3]

-- Fixed Costs --------------------------------------
		-- Sales Force costs (SF)  [SF]
		, [SF5000_B]
		, ([SF5000_B] + [SF5000_I]) AS [SF5000_C]
		, ([SF5000_B] + [SF5000_I]) AS [SF5000_F]
		, [SF5000C1]
		, [SF5000C2]
		, [SF5000C3]

/* BEGIN: To be removed from PBI then from this view */
		-- Sales Force costs People
		, CAST(NULL AS float) AS [SF5001_B]
		, CAST(NULL AS float) AS [SF5001_C]
		, CAST(NULL AS float) AS [SF5001_F]
		, CAST(NULL AS float) AS [SF5001C1]
		, CAST(NULL AS float) AS [SF5001C2]
		, CAST(NULL AS float) AS [SF5001C3]
		
		-- Sales Force costs Non-People
		, CAST(NULL AS float) AS [SF5002_B]
		, CAST(NULL AS float) AS [SF5002_C]
		, CAST(NULL AS float) AS [SF5002_F]
		, CAST(NULL AS float) AS [SF5002C1]
		, CAST(NULL AS float) AS [SF5002C2]
		, CAST(NULL AS float) AS [SF5002C3]
/* END */

		-- HOO Market excluding OPS  [HOO_MKT]
		, [HO5051_B]
		, ([HO5051_B] + [HO5051_I]) AS [HO5051_C]
		, ([HO5051_B] + [HO5051_I]) AS [HO5051_F]
		, [HO5051C1]
		, [HO5051C2]
		, [HO5051C3]

		-- HOO Operations  [HOO_OPS]
		, [HO5052_B]
		, ([HO5052_B] + [HO5052_I]) AS [HO5052_C]
		, ([HO5052_B] + [HO5052_I]) AS [HO5052_F]
		, [HO5052C1]
		, [HO5052C2]
		, [HO5052C3]

		-- HOO DBS  [HOO_DBS]
		, [HO5053_B]
		, ([HO5053_B] + [HO5053_I]) AS [HO5053_C]
		, ([HO5053_B] + [HO5053_I]) AS [HO5053_F]
		, [HO5053C1]
		, [HO5053C2]
		, [HO5053C3]

		-- HOO Global functions  [HOO_GLFUNC]
		, [HO5054_B]
		, ([HO5054_B] + [HO5054_I]) AS [HO5054_C]
		, ([HO5054_B] + [HO5054_I]) AS [HO5054_F]
		, [HO5054C1]
		, [HO5054C2]
		, [HO5054C3]

		-- Head Office Overheads (HOO)  [HOO_TOT]
		, [HO5051_B] + [HO5052_B] + [HO5053_B] + [HO5054_B] AS [HO5050_B]
		, ([HO5051_B] + [HO5051_I]) + ([HO5052_B] + [HO5052_I]) + ([HO5053_B] + [HO5053_I]) + ([HO5054_B] + [HO5054_I])	AS [HO5050_C]
		, ([HO5051_B] + [HO5051_I]) + ([HO5052_B] + [HO5052_I]) + ([HO5053_B] + [HO5053_I]) + ([HO5054_B] + [HO5054_I])	AS [HO5050_F]
		, [HO5051C1] + [HO5052C1] + [HO5053C1] + [HO5054C1] AS [HO5050C1]
		, [HO5051C2] + [HO5052C2] + [HO5053C2] + [HO5054C2] AS [HO5050C2]
		, [HO5051C3] + [HO5052C3] + [HO5053C3] + [HO5054C3] AS [HO5050C3]

		-- Total Overheads  [OVH_TOT] = [SF] + [HOO_TOT]
		, [SF5000_B]
		  + [HO5051_B] + [HO5052_B] + [HO5053_B] + [HO5054_B] AS [HO5090_B]
		, ([SF5000_B] + [SF5000_I])
		  + ([HO5051_B] + [HO5051_I]) + ([HO5052_B] + [HO5052_I]) + ([HO5053_B] + [HO5053_I]) + ([HO5054_B] + [HO5054_I]) AS [HO5090_C]
		, ([SF5000_B] + [SF5000_I])
		  + ([HO5051_B] + [HO5051_I]) + ([HO5052_B] + [HO5052_I]) + ([HO5053_B] + [HO5053_I]) + ([HO5054_B] + [HO5054_I]) AS [HO5090_F]
		, [SF5000C1]
		  + [HO5051C1] + [HO5052C1] + [HO5053C1] + [HO5054C1] AS [HO5090C1]
		, [SF5000C2]
		  + [HO5051C2] + [HO5052C2] + [HO5053C2] + [HO5054C2] AS [HO5090C2]
		, [SF5000C3]
		  + [HO5051C3] + [HO5052C3] + [HO5053C3] + [HO5054C3] AS [HO5090C3]
		
		-- Total OVH / NS (%)  [OVH_TOT_BY_NS]
		, (([SF5000_B] + [SF5000_I])
		  + ([HO5051_B] + [HO5051_I]) + ([HO5052_B] + [HO5052_I]) + ([HO5053_B] + [HO5053_I]) + ([HO5054_B] + [HO5054_I]))
		  / NULLIF([TL2030_B] + [TL2030_I], 0) AS [HO5790_F]

		-- Research & development costs (R&D)  [RND]
		, [RD6000_B]
		, ([RD6000_B] + [RD6000_I]) AS [RD6000_C]
		, ([RD6000_B] + [RD6000_I]) AS [RD6000_F]
		, [RD6000C1]
		, [RD6000C2]
		, [RD6000C3]

		-- Other Income and Expenses (OIE)  [OIE]
		, [IE7000_B]
		, ([IE7000_B] + [IE7000_I]) AS [IE7000_C]
		, ([IE7000_B] + [IE7000_I]) AS [IE7000_F]
		, [IE7000C1]
		, [IE7000C2]
		, [IE7000C3]

FROM [flex].[F_FLX_SCE_SIM];
GO

