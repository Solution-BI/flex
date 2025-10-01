USE SCHEMA COP_DMT_FLX;

MERGE INTO R_FLX_ETI USING (
    WITH
    _NEW_R_FLX_ETI (ETI_ELM_KEY,ETI_ELM_COD,ETI_ELM_DSC,CBU_COD,ETI_CRY_COD,ETI_CRY_DSC,ETI_CUR_COD) AS (SELECT * FROM VALUES
    /*** BEGIN DATA ***/
    ('DCH-DE01','DE01','Danone Deutschland GmbH'  ,'DCH','ETI_FNC_BU_BU1200','Germany'                                         ,'EUR'),
    ('IBE-ES02','ES02','Aguas Danone, S.A'        ,'IBE','ETI_FNC_BU_BU1100','Spain'                                           ,'EUR'),
    ('POL-PL02','PL02','ZYWIEC ZDROJ S.A.'        ,'POL','ETI_FNC_BU_BU1350','Poland'                                          ,'PLN'),
    ('POL-PL03','PL03','Nutricia Zaklady Pr'      ,'POL','ETI_FNC_BU_BU9414','Nutricia Zaklady Produkcyjne/Poland (Opole) Baby','PLN'),
    ('IBE-PT02','PT02','DANONE NUTRICIA, PORTUGAL','IBE','ETI_FNC_BU_BU1101','Portugal'                                        ,'EUR'),
    ('IBE-ES01','ES01','DANONE, S.A.'             ,'IBE','ETI_FNC_BU_BU1100','Spain'                                           ,'EUR'),
    ('POL-PL04','PL04','Nutrimed Sp. z o.o.'      ,'POL','ETI_FNC_BU_BU1350','Poland'                                          ,'PLN'),
    ('IBE-PT01','PT01','DANONE PORTUGAL, SA'      ,'IBE','ETI_FNC_BU_BU1101','Portugal'                                        ,'EUR'),
    ('POL-PL01','PL01','DANONE sp. z o.o.'        ,'POL','ETI_FNC_BU_BU1350','Poland'                                          ,'PLN'),
    ('DCH-CH01','CH01','Danone Schweiz AG'        ,'DCH','ETI_FNC_BU_BU1202','Switzerland'                                     ,'CHF'),
    ('IBE-ES03','ES03','DANONE NUTRICIA, S.R.L.'  ,'IBE','ETI_FNC_BU_BU1100','Spain'                                           ,'EUR'),
    ('POL-PL06','PL06','Promedica Sp. z o.o.'     ,'POL','ETI_FNC_BU_BU1350','Poland'                                          ,'PLN'),
    ('DCH-AT01','AT01','Danone Österreich GmbH'   ,'DCH','ETI_FNC_BU_BU1201','Austria'                                         ,'EUR'),
    ('POL-PL05','PL05','Nutricia Polska Sp. z o.o','POL','ETI_FNC_BU_BU1350','Poland'                                          ,'PLN'),
    ('DCH-0006','0006','DANONE GmbH'              ,'DCH','GEO_CRY_L3-DEU'   ,'DEU - Germany'                                   ,'EUR'),
    ('DCH-0013','0013','Danone Gesellschaft mbH'  ,'DCH','GEO_CRY_L3-AUT'   ,'AUT - Austria'                                   ,'EUR'),
    ('DCH-0048','0048','DANONE WATERS GERMANY'    ,'DCH','GEO_CRY_L3-DEU'   ,'DEU - Germany'                                   ,'EUR'),
    ('DCH-0071','0071','Danone AG'                ,'DCH','GEO_CRY_L3-CHE'   ,'CHE - Switzerland'                               ,'CHF'),
    ('DCH-0091','0091','Evian-Volvic Suisse SA'   ,'DCH','GEO_CRY_L3-CHE'   ,'CHE - Switzerland'                               ,'CHF'),
    ('DCH-4500','4500','Nutricia GmbH'            ,'DCH','GEO_CRY_L3-DEU'   ,'DEU - Germany'                                   ,'EUR'),
    ('DCH-4590','4590','Milupa GmbH'              ,'DCH','GEO_CRY_L3-AUT'   ,'AUT - Austria'                                   ,'EUR'),
    ('DCH-4610','4610','Milupa SA'                ,'DCH','GEO_CRY_L3-CHE'   ,'CHE - Switzerland'                               ,'CHF'),
    ('IBE-0036','0036','DANONE, S.A.'             ,'IBE','GEO_CRY_L3-ESP'   ,'ESP - Spain'                                     ,'EUR'),
    ('IBE-0051','0051','DANONE PORTUGAL, SA'      ,'IBE','GEO_CRY_L3-PRT'   ,'PRT - Portugal'                                  ,'EUR'),
    ('IBE-0052','0052','Aguas Danone, S.A'        ,'IBE','GEO_CRY_L3-ESP'   ,'ESP - Spain'                                     ,'EUR'),
    ('IBE-4660','4660','Nutricia S.R.L.'          ,'IBE','GEO_CRY_L3-ESP'   ,'ESP - Spain'                                     ,'EUR'),
    ('IBE-4780','4780','NUTRICIA AMN - Unipessoal','IBE','GEO_CRY_L3-PRT'   ,'PRT - Portugal'                                  ,'EUR')
    /*** END DATA ***/
    ),
    _ALL_R_FLX_ETI (ETI_ELM_KEY,ETI_ELM_COD,ETI_ELM_DSC,CBU_COD,ETI_CRY_COD,ETI_CRY_DSC,ETI_CUR_COD, T_REC_DLT_FLG) AS (
        SELECT *, 0 AS T_REC_DLT_FLG FROM _NEW_R_FLX_ETI
        UNION ALL
        SELECT ETI_ELM_KEY,ETI_ELM_COD,ETI_ELM_DSC,CBU_COD,ETI_CRY_COD,ETI_CRY_DSC,ETI_CUR_COD, 1 AS T_REC_DLT_FLG
        FROM R_FLX_ETI
        WHERE NOT EXISTS (SELECT NULL
                          FROM   _NEW_R_FLX_ETI
                          WHERE  R_FLX_ETI.ETI_ELM_KEY = _NEW_R_FLX_ETI.ETI_ELM_KEY
                         )
    )
    SELECT * FROM _ALL_R_FLX_ETI

) AS _R_FLX_ETI ON (R_FLX_ETI.ETI_ELM_KEY = _R_FLX_ETI.ETI_ELM_KEY)
    WHEN MATCHED AND _R_FLX_ETI.T_REC_DLT_FLG = 1 THEN 
         DELETE
    WHEN MATCHED AND _R_FLX_ETI.T_REC_DLT_FLG = 0 THEN 
         UPDATE SET R_FLX_ETI.ETI_CUR_COD   = _R_FLX_ETI.ETI_CUR_COD
                   ,R_FLX_ETI.T_REC_UPD_TST = current_timestamp
    WHEN NOT MATCHED THEN 
         INSERT (           ETI_ELM_KEY,            ETI_ELM_COD,            ETI_ELM_DSC,            CBU_COD,            ETI_CRY_COD,            ETI_CRY_DSC,            ETI_CUR_COD, T_REC_DLT_FLG,     T_REC_INS_TST,     T_REC_UPD_TST)
         VALUES (_R_FLX_ETI.ETI_ELM_KEY, _R_FLX_ETI.ETI_ELM_COD, _R_FLX_ETI.ETI_ELM_DSC, _R_FLX_ETI.CBU_COD, _R_FLX_ETI.ETI_CRY_COD, _R_FLX_ETI.ETI_CRY_DSC, _R_FLX_ETI.ETI_CUR_COD,             0, current_timestamp, current_timestamp)
;
