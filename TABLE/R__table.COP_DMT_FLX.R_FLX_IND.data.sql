USE SCHEMA COP_DMT_FLX;

MERGE INTO R_FLX_IND USING (
    WITH
    _NEW_R_FLX_IND (IND_ELM_COD, IND_ELM_DSC, IND_ELM_DSP_DSC, IND_ELM_TEK_COD, IND_ORD_NUM, PAR_IND_ELM_COD, IND_FML_TXT, FCA_IND_ELM_COD, VAR_IND_ELM_COD) AS (SELECT * FROM VALUES
    /*** BEGIN DATA ***/
    ('VOL'               , 'Volume sold'                                                , 'Volume'                      , 'VL1000',  1, '<top>'  , 'VOL_DRY | VOL_KLITERS | VOL_TONS | VOL_UNITS', ''   , ''   ),
    ('NS'                , 'Net Sales (NS)'                                             , 'Net Sales'                   , 'TL2030',  2, 'GP'     , ''                                            , ''   , 'VOL'),
    ('NS_BY_VOL'         , 'Net Sales / Volume sold'                                    , 'NS / Vol'                    , 'TL2930',  3, ''       , 'NS / VOL'                                    , ''   , ''   ),
    ('MAT_COS'           , 'Material Cost of Sales'                                     , 'Material Cost of Sales'      , 'CG3001',  4, 'MAT'    , ''                                            , ''   , 'VOL'),
    ('MAT_COS_BY_VOL'    , 'Material Cost of Sales / Volume sold'                       , 'Material Cost of Sales / Vol', 'CG3901',  5, ''       , 'MAT_COS / VOL'                               , ''   , ''   ),
    ('MAT_OTH'           , 'Rest of Material Costs'                                     , 'Rest of Material Costs'      , 'CG3002',  6, 'MAT'    , ''                                            , 'VOL', 'VOL'),
    ('MAT_OTH_BY_VOL'    , 'Rest of Material Costs / Volume sold'                       , 'Rest of Material Costs / Vol', 'CG3902',  7, ''       , 'MAT_OTH / VOL'                               , ''   , ''   ),
    ('MAT'               , 'Material Costs'                                             , 'Material Costs'              , 'CG3000',  8, 'COGS'   , ''                                            , ''   , ''   ),
    ('MAT_BY_VOL'        , 'Material Costs / Volume sold'                               , 'Mat. Costs / Vol'            , 'CG3900',  9, ''       , 'MAT / VOL'                                   , ''   , ''   ),
    ('MANUF_COS'         , 'Manufacturing Cost of Sales'                                , 'Manuf. Cost of Sales'        , 'CG3011', 10, 'MANUF'  , ''                                            , ''   , 'VOL'),
    ('MANUF_COS_BY_VOL'  , 'Manufacturing Cost of Sales / Volume sold'                  , 'Manuf. Cost of Sales / Vol'  , 'CG3911', 11, ''       , 'MANUF_COS / VOL'                             , ''   , ''   ),
    ('MANUF_OTH'         , 'Rest of Manufacturing Costs'                                , 'Rest of Manuf. Costs'        , 'CG3012', 12, 'MANUF'  , ''                                            , 'VOL', 'VOL'),
    ('MANUF_OTH_BY_VOL'  , 'Rest of Manufacturing Costs / Volume sold'                  , 'Rest of Manuf. Costs / Vol'  , 'CG3912', 13, ''       , 'MANUF_OTH / VOL'                             , ''   , ''   ),
    ('MANUF'             , 'Manufacturing Costs'                                        , 'Manufacturing Costs'         , 'CG3010', 14, 'COGS'   , ''                                            , ''   , ''   ),
    ('MANUF_BY_VOL'      , 'Manufacturing Costs / Volume sold'                          , 'Manuf. Costs / Vol'          , 'CG3910', 15, ''       , 'MANUF / VOL'                                 , ''   , ''   ),
    ('LOG_FTC_IFO'       , 'Freight to Customers and Internal Freight Out'              , 'FTC and IFO'                 , 'CG3021', 16, 'LOG'    , ''                                            , ''   , 'VOL'),
    ('LOG_FTC_IFO_BY_VOL', 'Freight to Customers and Internal Freight Out / Volume sold', 'FTC and IFO / Vol'           , 'CG3921', 17, ''       , 'LOG_FTC_IFO / VOL'                           , ''   , ''   ),
    ('LOG_USL'           , 'Unsaleable'                                                 , 'Unsaleable'                  , 'CG3022', 18, 'LOG'    , ''                                            , ''   , 'VOL'),
    ('LOG_USL_BY_VOL'    , 'Unsaleable / Volume sold'                                   , 'Unsaleable / Vol'            , 'CG3922', 19, ''       , 'LOG_USL / VOL'                               , ''   , ''   ),
    ('LOG_OTH'           , 'Rest of Logistic Costs'                                     , 'Rest of Log. Costs'          , 'CG3023', 20, 'LOG'    , ''                                            , 'VOL', 'VOL'),
    ('LOG_OTH_BY_VOL'    , 'Rest of Logistic Costs / Volume sold'                       , 'Rest of Log. Costs / Vol'    , 'CG3923', 21, ''       , 'LOG_OTH / VOL'                               , ''   , ''   ),
    ('LOG'               , 'Logistic Costs'                                             , 'Logistic Costs'              , 'CG3020', 22, 'COGS'   , ''                                            , ''   , ''   ),
    ('LOG_BY_VOL'        , 'Logistic Costs / Volume sold'                               , 'Log. Costs / Vol'            , 'CG3920', 23, ''       , 'LOG / VOL'                                   , ''   , ''   ),
    ('COGS'              , 'Total COGS (Cost of Goods Sold)'                            , 'COGS'                        , 'CG3030', 24, 'GP'     , ''                                            , ''   , ''   ),
    ('COGS_BY_VOL'       , 'Cost of Goods Sold (COGS) / Volume sold'                    , 'COGS / Vol'                  , 'CG3930', 25, ''       , 'COGS / VOL'                                  , ''   , ''   ),
    ('GP'                , 'Gross Profit'                                               , 'Gross Profit'                , 'CG3040', 26, 'PM'     , ''                                            , ''   , ''   ),
    ('GP_BY_NS'          , 'Gross margin %'                                             , 'Gross Margin'                , 'CG3740', 27, ''       , 'GP / NS'                                     , ''   , ''   ),
    ('GP_BY_VOL'         , 'GP / Vol'                                                   , 'GP / Vol'                    , 'CG3940', 28, ''       , 'GP / VOL'                                    , ''   , ''   ),
    ('AP_WRK'            , 'A&P Working'                                                , 'A&P Working'                 , 'AP4001', 29, 'AP'     , ''                                            , ''   , ''   ),
    ('AP_WRK_BY_AP'      , 'A&P Working / Total A&P (%)'                                , 'A&P Working / A&P'           , 'AP4501', 30, ''       , 'AP_WRK / AP'                                 , ''   , ''   ),
    ('AP_NON_WRK'        , 'A&P Non Working'                                            , 'A&P Non Working'             , 'AP4002', 31, 'AP'     , ''                                            , ''   , ''   ),
    ('AP_OTH'            , 'Other A&P'                                                  , 'Other A&P'                   , 'AP4003', 32, 'AP'     , ''                                            , ''   , ''   ),
    ('AP'                , 'Marketing Costs (A&P)'                                      , 'A&P'                         , 'AP4000', 33, 'PM'     , ''                                            , ''   , ''   ),
    ('AP_BY_NS'          , 'A&P / NS (%)'                                               , 'A&P / NS'                    , 'AP4700', 34, ''       , 'AP / NS'                                     , ''   , ''   ),
    ('PM'                , 'Product Margin (PM)'                                        , 'PM'                          , 'AP4010', 35, 'CM'     , ''                                            , ''   , ''   ),
    ('PM_BY_NS'          , 'PM / NS (%)'                                                , 'PM / NS'                     , 'AP4710', 36, ''       , 'PM / NS'                                     , ''   , ''   ),
    ('SF'                , 'Sales Force Costs (SF)'                                     , 'SFC'                         , 'SF5000', 37, 'CM'     , ''                                            , ''   , ''   ),
    ('SF_BY_NS'          , 'SF / NS (%)'                                                , 'SFC / NS'                    , 'SF5700', 38, ''       , 'SF / NS'                                     , ''   , ''   ),
    ('CM'                , 'Channel Margin (CM)'                                        , 'CM'                          , 'SF5010', 39, 'ROP'    , ''                                            , ''   , ''   ),
    ('CM_BY_NS'          , 'CM / NS (%)'                                                , 'CM / NS'                     , 'SF5710', 40, ''       , 'CM / NS'                                     , ''   , ''   ),
    ('HOO_MKT'           , 'HOO Market excluding OPS'                                   , 'HOO Market Excl. OPS'        , 'HO5051', 41, 'HOO_TOT', ''                                            , ''   , ''   ),
    ('HOO_OPS'           , 'HOO Operations'                                             , 'HOO OPS'                     , 'HO5052', 42, 'HOO_TOT', ''                                            , ''   , ''   ),
    ('HOO_DBS'           , 'HOO DBS'                                                    , 'HOO DBS'                     , 'HO5053', 43, 'HOO_TOT', ''                                            , ''   , ''   ),
    ('HOO_GLFUNC'        , 'HOO Global Functions'                                       , 'HOO Global Functions'        , 'HO5054', 44, 'HOO_TOT', ''                                            , ''   , ''   ),
    ('HOO_TOT'           , 'Head Office Overheads (HOO)'                                , 'HOO'                         , 'HO5050', 45, 'ROP'    , ''                                            , ''   , ''   ),
    ('HOO_TOT_BY_NS'     , 'HOO / NS (%)'                                               , 'HOO / NS'                    , 'HO5750', 46, ''       , 'HOO_TOT / NS'                                , ''   , ''   ),
    ('OVH_TOT'           , 'Total OVH'                                                  , 'Total OVH'                   , 'HO5090', 47, ''       , 'SF + HOO_TOT'                                , ''   , ''   ),
    ('OVH_TOT_BY_NS'     , 'Total OVH / NS (%)'                                         , 'Total OVH / NS'              , 'HO5790', 48, ''       , 'OVH_TOT / NS'                                , ''   , ''   ),
    ('RND'               , 'Research & development costs (R&D)'                         , 'R&D'                         , 'RD6000', 49, 'ROP'    , ''                                            , ''   , ''   ),
    ('RND_BY_NS'         , 'R&D / NS (%)'                                               , 'R&D / NS'                    , 'RD6700', 50, ''       , 'RND / NS'                                    , ''   , ''   ),
    ('OIE'               , 'Other Income and Expenses (OIE)'                            , 'OIE'                         , 'IE7000', 51, 'ROP'    , ''                                            , ''   , ''   ),
    ('OIE_BY_NS'         , 'OIE / NS (%)'                                               , 'OIE / NS'                    , 'IE7700', 52, ''       , 'OIE / NS'                                    , ''   , ''   ),
    ('ROP'               , 'ROP (Trading Operating Income)'                             , 'ROP'                         , 'OI9000', 53, '<top>'  , ''                                            , ''   , ''   ),
    ('ROP_BY_NS'         , 'ROS (%)'                                                    , 'ROS'                         , 'OI9700', 54, ''       , 'ROP / NS'                                    , ''   , ''   )
    /*** END DATA ***/
    ),
    _ALL_R_FLX_IND (IND_ELM_COD, IND_ELM_DSC, IND_ELM_DSP_DSC, IND_ELM_TEK_COD, IND_ORD_NUM, PAR_IND_ELM_COD, IND_FML_TXT, FCA_IND_ELM_COD, VAR_IND_ELM_COD, T_REC_DLT_FLG) AS (
        SELECT *, 0 AS T_REC_DLT_FLG FROM _NEW_R_FLX_IND
        UNION ALL
        SELECT IND_ELM_COD, IND_ELM_DSC, IND_ELM_DSP_DSC, IND_ELM_TEK_COD, IND_ORD_NUM, PAR_IND_ELM_COD, IND_FML_TXT, FCA_IND_ELM_COD, VAR_IND_ELM_COD, 1 AS T_REC_DLT_FLG
        FROM R_FLX_IND
        WHERE NOT EXISTS (
            SELECT 1
            FROM _NEW_R_FLX_IND
            WHERE R_FLX_IND.IND_ELM_COD = _NEW_R_FLX_IND.IND_ELM_COD
        )
    )
    SELECT * FROM _ALL_R_FLX_IND

) AS _R_FLX_IND ON (R_FLX_IND.IND_ELM_COD = _R_FLX_IND.IND_ELM_COD)
    WHEN MATCHED AND _R_FLX_IND.T_REC_DLT_FLG = 1
        THEN DELETE
    WHEN MATCHED AND _R_FLX_IND.T_REC_DLT_FLG = 0 AND (
            R_FLX_IND.IND_ELM_DSC     IS DISTINCT FROM _R_FLX_IND.IND_ELM_DSC OR
            R_FLX_IND.IND_ELM_DSP_DSC IS DISTINCT FROM _R_FLX_IND.IND_ELM_DSP_DSC OR
            R_FLX_IND.IND_ELM_TEK_COD IS DISTINCT FROM _R_FLX_IND.IND_ELM_TEK_COD OR
            R_FLX_IND.IND_ORD_NUM     IS DISTINCT FROM _R_FLX_IND.IND_ORD_NUM OR
            R_FLX_IND.PAR_IND_ELM_COD IS DISTINCT FROM _R_FLX_IND.PAR_IND_ELM_COD OR
            R_FLX_IND.IND_FML_TXT     IS DISTINCT FROM _R_FLX_IND.IND_FML_TXT OR
            R_FLX_IND.FCA_IND_ELM_COD IS DISTINCT FROM _R_FLX_IND.FCA_IND_ELM_COD OR
            R_FLX_IND.VAR_IND_ELM_COD IS DISTINCT FROM _R_FLX_IND.VAR_IND_ELM_COD
        )
        THEN UPDATE SET R_FLX_IND.IND_ELM_DSC = _R_FLX_IND.IND_ELM_DSC
                      , R_FLX_IND.IND_ORD_NUM = _R_FLX_IND.IND_ORD_NUM
                      , R_FLX_IND.PAR_IND_ELM_COD = _R_FLX_IND.PAR_IND_ELM_COD
                      , R_FLX_IND.IND_FML_TXT = _R_FLX_IND.IND_FML_TXT
                      , R_FLX_IND.T_REC_UPD_TST = current_timestamp
    WHEN NOT MATCHED
        THEN INSERT (           IND_ELM_COD,            IND_ELM_DSC,            IND_ELM_DSP_DSC,            IND_ELM_TEK_COD,            IND_ORD_NUM,            PAR_IND_ELM_COD,            IND_FML_TXT,            FCA_IND_ELM_COD,            VAR_IND_ELM_COD, T_REC_ARC_FLG, T_REC_DLT_FLG,     T_REC_SRC_TST,     T_REC_INS_TST,     T_REC_UPD_TST)
             VALUES (_R_FLX_IND.IND_ELM_COD, _R_FLX_IND.IND_ELM_DSC, _R_FLX_IND.IND_ELM_DSP_DSC, _R_FLX_IND.IND_ELM_TEK_COD, _R_FLX_IND.IND_ORD_NUM, _R_FLX_IND.PAR_IND_ELM_COD, _R_FLX_IND.IND_FML_TXT, _R_FLX_IND.FCA_IND_ELM_COD, _R_FLX_IND.VAR_IND_ELM_COD,             0,             0, current_timestamp, current_timestamp, current_timestamp);
