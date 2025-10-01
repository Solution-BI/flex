USE SCHEMA COP_DMT_FLX;

CREATE OR REPLACE PROCEDURE SP_Flex_Load_Product_From_CC()
   RETURNS VARCHAR
   LANGUAGE SQL
   EXECUTE AS OWNER
AS
$$
/*
=========================================================================
                                HISTORY
=========================================================================
Description : Create Procedure Script to load data in R_FLX_PDT
Author      : COQUIO Noel (Solution BI France)                      
Created On  : 01-07-2024                                                 
=========================================================================
Modified On:      Description:                                 Author:          
07-10-2024        Add NA, OTH, ND & OTHERS materials           COQUIO Noel
11-12-2024        Use Lx_PRODUCT_CATEGORY_BRAND hierarchy      COQUIO Noel
                  instead of LX_PRODUCT_CATEGORY
06-05-2025        Added pdt.T_REC_DLT_FLG = 0                  SHUDDHO Manan
23-05-2025        Added LL to Product Description              SHUDDHO Manan
=========================================================================
*/

DECLARE
v_STEP_TABLE     VARCHAR(256);

BEGIN

   -- Load the table R_FLX_PDT in incremental mode

   v_STEP_TABLE := 'R_FLX_PDT';

   UPDATE COP_DMT_FLX.R_FLX_PDT FLEX SET T_REC_DLT_FLG = 1;

   MERGE INTO COP_DMT_FLX.R_FLX_PDT FLEX USING (SELECT CBU_COD || '-' || PRODUCT_CODE                                        PDT_ELM_KEY
                                                      ,PRODUCT_CODE                                                          PDT_ELM_COD
                                                      ,PRODUCT_DESC  || ' (LL)'                                              PDT_ELM_DSC
                                                      ,CBU_COD                                                               CBU_COD
                                             
                                                      ,L1_PLATFORM_BRAND_CODE                                                BRD_HIE_LV1_PTF_BRD_COD
                                                      ,L1_PLATFORM_BRAND_DESC || ' (PDT03 - L1)'                             BRD_HIE_LV1_PTF_BRD_DSC
                                                      ,L2_BRAND_CODE                                                         BRD_HIE_LV2_BRD_COD
                                                      ,L2_BRAND_DESC || ' (PDT03 - L2)'                                      BRD_HIE_LV2_BRD_DSC
                                                      ,L3_SUB_BRAND_CODE                                                     BRD_HIE_LV3_SUB_BRD_COD
                                                      ,L3_SUB_BRAND_DESC || ' (PDT03 - L3)'                                  BRD_HIE_LV3_SUB_BRD_DSC

                                                      ,DECODE(CBU_COD 
                                                             ,'DCH',L1_DA_PRODUCT_CATEGORY_CODE
                                                             ,'IBE',L1_IBE_PRODUCT_CATEGORY_CODE
                                                             ,'POL',L1_POL_PRODUCT_CATEGORY_CODE
                                                             )                                                               LCL_HIE_LV1_PDT_ARE_COD
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L1_DA_PRODUCT_CATEGORY_DESC
                                                             ,'IBE',L1_IBE_PRODUCT_CATEGORY_DESC
                                                             ,'POL',L1_POL_PRODUCT_CATEGORY_DESC
                                                             ) || ' (PHL - L1)'                                              LCL_HIE_LV1_PDT_ARE_DSC
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L2_DA_PRODUCT_CATEGORY_CODE
                                                             ,'IBE',L2_IBE_PRODUCT_CATEGORY_CODE
                                                             ,'POL',L2_POL_PRODUCT_CATEGORY_CODE
                                                             )                                                               LCL_HIE_LV2_UMB_BRD_COD
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L2_DA_PRODUCT_CATEGORY_DESC
                                                             ,'IBE',L2_IBE_PRODUCT_CATEGORY_DESC
                                                             ,'POL',L2_POL_PRODUCT_CATEGORY_DESC
                                                             ) || ' (PHL - L2)'                                              LCL_HIE_LV2_UMB_BRD_DSC
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L3_DA_PRODUCT_CATEGORY_CODE
                                                             ,'IBE',L3_IBE_PRODUCT_CATEGORY_CODE
                                                             ,'POL',L3_POL_PRODUCT_CATEGORY_CODE
                                                             )                                                               LCL_HIE_LV3_PDT_BRD_COD
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L3_DA_PRODUCT_CATEGORY_DESC
                                                             ,'IBE',L3_IBE_PRODUCT_CATEGORY_DESC
                                                             ,'POL',L3_POL_PRODUCT_CATEGORY_DESC
                                                             ) || ' (PHL - L3)'                                              LCL_HIE_LV3_PDT_BRD_DSC
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L4_DA_PRODUCT_CATEGORY_CODE
                                                             ,'IBE',L4_IBE_PRODUCT_CATEGORY_CODE
                                                             ,'POL',L4_POL_PRODUCT_CATEGORY_CODE
                                                             )                                                               LCL_HIE_LV4_PDT_FAM_COD
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L4_DA_PRODUCT_CATEGORY_DESC
                                                             ,'IBE',L4_IBE_PRODUCT_CATEGORY_DESC
                                                             ,'POL',L4_POL_PRODUCT_CATEGORY_DESC
                                                             ) || ' (PHL - L4)'                                              LCL_HIE_LV4_PDT_FAM_DSC
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L5_DA_PRODUCT_CATEGORY_CODE
                                                             ,'IBE',L5_IBE_PRODUCT_CATEGORY_CODE
                                                             ,'POL',L5_POL_PRODUCT_CATEGORY_CODE
                                                             )                                                               LCL_HIE_LV5_PDT_SFM_COD
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L5_DA_PRODUCT_CATEGORY_DESC
                                                             ,'IBE',L5_IBE_PRODUCT_CATEGORY_DESC
                                                             ,'POL',L5_POL_PRODUCT_CATEGORY_DESC
                                                             ) || ' (PHL - L5)'                                              LCL_HIE_LV5_PDT_SFM_DSC
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L6_DA_PRODUCT_CATEGORY_CODE
                                                             ,'IBE',L6_IBE_PRODUCT_CATEGORY_CODE
                                                             ,'POL',L6_POL_PRODUCT_CATEGORY_CODE
                                                             )                                                               LCL_HIE_LV6_PDT_NAT_COD
                                                      ,DECODE(CBU_COD
                                                             ,'DCH',L6_DA_PRODUCT_CATEGORY_DESC
                                                             ,'IBE',L6_IBE_PRODUCT_CATEGORY_DESC
                                                             ,'POL',L6_POL_PRODUCT_CATEGORY_DESC
                                                             ) || ' (PHL - L6)'                                              LCL_HIE_LV6_PDT_NAT_DSC

                                                      ,L0_PRODUCT_CATEGORY_CODE                                              LV0_PDT_CAT_COD
                                                      ,L0_PRODUCT_CATEGORY_DESC                                              LV0_PDT_CAT_DSC

                                                      ,L1_PRODUCT_CATEGORY_BRAND_CODE                                        LV1_PDT_ARE_COD
                                                      ,L1_PRODUCT_CATEGORY_BRAND_DESC || ' (PDT01-L1)'                       LV1_PDT_ARE_DSC
                                                      ,L2_PRODUCT_CATEGORY_BRAND_CODE                                        LV2_UMB_BRD_COD
                                                      ,L2_PRODUCT_CATEGORY_BRAND_DESC || ' (PDT01-L2)'                       LV2_UMB_BRD_DSC
                                                      ,L3_PRODUCT_CATEGORY_BRAND_CODE                                        LV3_PDT_BRD_COD
                                                      ,L3_PRODUCT_CATEGORY_BRAND_DESC || ' (PDT01-L3)'                       LV3_PDT_BRD_DSC
                                                      ,L4_PRODUCT_CATEGORY_BRAND_CODE                                        LV4_PDT_FAM_COD
                                                      ,L4_PRODUCT_CATEGORY_BRAND_DESC || ' (PDT01-L4)'                       LV4_PDT_FAM_DSC
                                                      ,L5_PRODUCT_CATEGORY_BRAND_CODE                                        LV5_PDT_SFM_COD
                                                      ,L5_PRODUCT_CATEGORY_BRAND_DESC || ' (PDT01-L5)'                       LV5_PDT_SFM_DSC
                                                      ,L6_PRODUCT_CATEGORY_BRAND_CODE                                        LV6_PDT_NAT_COD
                                                      ,L6_PRODUCT_CATEGORY_BRAND_DESC || ' (PDT01-L6)'                       LV6_PDT_NAT_DSC
                                                      ,0                                                                     T_REC_DLT_FLG
                                                      ,CURRENT_TIMESTAMP                                                     T_REC_INS_TST
                                                      ,CURRENT_TIMESTAMP                                                     T_REC_UPD_TST
                                                FROM   PRD_COP.COP_DSP_CONTROLLING_CLOUD.R_PRODUCT_UNIFY
                                                WHERE  TRY_CAST(PRODUCT_CODE AS NUMBER) IS NOT NULL
                                                OR     PRODUCT_CODE                     IN ('NA','OTH','ND','OTHERS','L1_Z0')
                                                UNION 
-- Include missing data from the PRD_COP.COP_DSP_CONTROLLING_CLOUD.R_PRODUCT_UNIFY views and presents in Fact table
                                                SELECT * 
                                                FROM   (SELECT 'DCH-181837'                         PDT_ELM_KEY
                                                              ,'181837'                             PDT_ELM_COD
                                                              ,'OIKOS MIX 3STRAW/3RASP 4x115g(x6)' 
                                                              || ' (LL)'                            PDT_ELM_DSC
                                                              ,'DCH'                                CBU_COD
                                                              ,'L1_C8'        BRD_HIE_LV1_PTF_BRD_COD,'Oikos (PDT03 - L1)'          BRD_HIE_LV1_PTF_BRD_DSC
                                                              ,'L2_C8179'     BRD_HIE_LV2_BRD_COD    ,'Oikos (PDT03 - L2)'          BRD_HIE_LV2_BRD_DSC
                                                              ,'L3_C81790263' BRD_HIE_LV3_SUB_BRD_COD,'Oikos (PDT03 - L3)'          BRD_HIE_LV3_SUB_BRD_DSC
                                                              ,'L1_NC'        LCL_HIE_LV1_PDT_ARE_COD,'Not Classified (PHL - L1)'   LCL_HIE_LV1_PDT_ARE_DSC
                                                              ,'L2_NC'        LCL_HIE_LV2_UMB_BRD_COD,'Not Classified (PHL - L2)'   LCL_HIE_LV2_UMB_BRD_DSC
                                                              ,'L3_NC'        LCL_HIE_LV3_PDT_BRD_COD,'Not Classified (PHL - L3)'   LCL_HIE_LV3_PDT_BRD_DSC
                                                              ,'L4_NC'        LCL_HIE_LV4_PDT_FAM_COD,'Not Classified (PHL - L4)'   LCL_HIE_LV4_PDT_FAM_DSC
                                                              ,'L5_NC'        LCL_HIE_LV5_PDT_SFM_COD,'Not Classified (PHL - L5)'   LCL_HIE_LV5_PDT_SFM_DSC
                                                              ,'L6_NC'        LCL_HIE_LV6_PDT_NAT_COD,'Not Classified (PHL - L6)'   LCL_HIE_LV6_PDT_NAT_DSC
                                                              ,'NA'           LV0_PDT_CAT_COD        ,'NA'                          LV0_PDT_CAT_DSC
                                                              ,'NA'           LV1_PDT_ARE_COD        ,'NA (PDT01-L1)'               LV1_PDT_ARE_DSC
                                                              ,'NA'           LV2_UMB_BRD_COD        ,'NA (PDT01-L2)'               LV2_UMB_BRD_DSC
                                                              ,'NA'           LV3_PDT_BRD_COD        ,'NA (PDT01-L3)'               LV3_PDT_BRD_DSC
                                                              ,'NA'           LV4_PDT_FAM_COD        ,'NA (PDT01-L4)'               LV4_PDT_FAM_DSC
                                                              ,'NA'           LV5_PDT_SFM_COD        ,'NA (PDT01-L5)'               LV5_PDT_SFM_DSC
                                                              ,'NA'           LV6_PDT_NAT_COD        ,'NA (PDT01-L6)'               LV6_PDT_NAT_DSC
                                                              ,0 T_REC_DLT_FLG,CURRENT_TIMESTAMP T_REC_INS_TST,CURRENT_TIMESTAMP T_REC_UPD_TST 
                                                        UNION ALL
                                                        SELECT 'DCH-181838'                         PDT_ELM_KEY
                                                              ,'181838'                             PDT_ELM_COD
                                                              ,'OIKOS MIX 3VAN/3STRACIA 4x115g(x6)'
                                                              || ' (LL)'                            PDT_ELM_DSC
                                                              ,'DCH'                                CBU_COD
                                                              ,'L1_C8'        BRD_HIE_LV1_PTF_BRD_COD,'Oikos (PDT03 - L1)'          BRD_HIE_LV1_PTF_BRD_DSC
                                                              ,'L2_C8179'     BRD_HIE_LV2_BRD_COD    ,'Oikos (PDT03 - L2)'          BRD_HIE_LV2_BRD_DSC
                                                              ,'L3_C81790263' BRD_HIE_LV3_SUB_BRD_COD,'Oikos (PDT03 - L3)'          BRD_HIE_LV3_SUB_BRD_DSC
                                                              ,'L1_NC'        LCL_HIE_LV1_PDT_ARE_COD,'Not Classified (PHL - L1)'   LCL_HIE_LV1_PDT_ARE_DSC
                                                              ,'L2_NC'        LCL_HIE_LV2_UMB_BRD_COD,'Not Classified (PHL - L2)'   LCL_HIE_LV2_UMB_BRD_DSC
                                                              ,'L3_NC'        LCL_HIE_LV3_PDT_BRD_COD,'Not Classified (PHL - L3)'   LCL_HIE_LV3_PDT_BRD_DSC
                                                              ,'L4_NC'        LCL_HIE_LV4_PDT_FAM_COD,'Not Classified (PHL - L4)'   LCL_HIE_LV4_PDT_FAM_DSC
                                                              ,'L5_NC'        LCL_HIE_LV5_PDT_SFM_COD,'Not Classified (PHL - L5)'   LCL_HIE_LV5_PDT_SFM_DSC
                                                              ,'L6_NC'        LCL_HIE_LV6_PDT_NAT_COD,'Not Classified (PHL - L6)'   LCL_HIE_LV6_PDT_NAT_DSC
                                                              ,'NA'           LV0_PDT_CAT_COD        ,'NA'                          LV0_PDT_CAT_DSC
                                                              ,'NA'           LV1_PDT_ARE_COD        ,'NA (PDT01-L1)'               LV1_PDT_ARE_DSC
                                                              ,'NA'           LV2_UMB_BRD_COD        ,'NA (PDT01-L2)'               LV2_UMB_BRD_DSC
                                                              ,'NA'           LV3_PDT_BRD_COD        ,'NA (PDT01-L3)'               LV3_PDT_BRD_DSC
                                                              ,'NA'           LV4_PDT_FAM_COD        ,'NA (PDT01-L4)'               LV4_PDT_FAM_DSC
                                                              ,'NA'           LV5_PDT_SFM_COD        ,'NA (PDT01-L5)'               LV5_PDT_SFM_DSC
                                                              ,'NA'           LV6_PDT_NAT_COD        ,'NA (PDT01-L6)'               LV6_PDT_NAT_DSC
                                                              ,0 T_REC_DLT_FLG,CURRENT_TIMESTAMP T_REC_INS_TST,CURRENT_TIMESTAMP T_REC_UPD_TST 
                                                        UNION ALL
                                                        SELECT 'DCH-192325'                         PDT_ELM_KEY
                                                              ,'192325'                             PDT_ELM_COD
                                                              ,'YOPRO DUMMY 2' || ' (LL)'           PDT_ELM_DSC
                                                              ,'DCH'                                CBU_COD
                                                              ,'L1_B1'        BRD_HIE_LV1_PTF_BRD_COD,'YoPRO (PDT03 - L1)'          BRD_HIE_LV1_PTF_BRD_DSC
                                                              ,'L2_B1208'     BRD_HIE_LV2_BRD_COD    ,'YoPRO (PDT03 - L2)'          BRD_HIE_LV2_BRD_DSC
                                                              ,'L3_B12080295' BRD_HIE_LV3_SUB_BRD_COD,'YoPRO (PDT03 - L3)'          BRD_HIE_LV3_SUB_BRD_DSC
                                                              ,'L1_NC'        LCL_HIE_LV1_PDT_ARE_COD,'Not Classified (PHL - L1)'   LCL_HIE_LV1_PDT_ARE_DSC
                                                              ,'L2_NC'        LCL_HIE_LV2_UMB_BRD_COD,'Not Classified (PHL - L2)'   LCL_HIE_LV2_UMB_BRD_DSC
                                                              ,'L3_NC'        LCL_HIE_LV3_PDT_BRD_COD,'Not Classified (PHL - L3)'   LCL_HIE_LV3_PDT_BRD_DSC
                                                              ,'L4_NC'        LCL_HIE_LV4_PDT_FAM_COD,'Not Classified (PHL - L4)'   LCL_HIE_LV4_PDT_FAM_DSC
                                                              ,'L5_NC'        LCL_HIE_LV5_PDT_SFM_COD,'Not Classified (PHL - L5)'   LCL_HIE_LV5_PDT_SFM_DSC
                                                              ,'L6_NC'        LCL_HIE_LV6_PDT_NAT_COD,'Not Classified (PHL - L6)'   LCL_HIE_LV6_PDT_NAT_DSC
                                                              ,'NA'           LV0_PDT_CAT_COD        ,'NA'                          LV0_PDT_CAT_DSC
                                                              ,'NA'           LV1_PDT_ARE_COD        ,'NA (PDT01-L1)'               LV1_PDT_ARE_DSC
                                                              ,'NA'           LV2_UMB_BRD_COD        ,'NA (PDT01-L2)'               LV2_UMB_BRD_DSC
                                                              ,'NA'           LV3_PDT_BRD_COD        ,'NA (PDT01-L3)'               LV3_PDT_BRD_DSC
                                                              ,'NA'           LV4_PDT_FAM_COD        ,'NA (PDT01-L4)'               LV4_PDT_FAM_DSC
                                                              ,'NA'           LV5_PDT_SFM_COD        ,'NA (PDT01-L5)'               LV5_PDT_SFM_DSC
                                                              ,'NA'           LV6_PDT_NAT_COD        ,'NA (PDT01-L6)'               LV6_PDT_NAT_DSC
                                                              ,0 T_REC_DLT_FLG,CURRENT_TIMESTAMP T_REC_INS_TST,CURRENT_TIMESTAMP T_REC_UPD_TST 
                                                        ) missing_
                                                WHERE  NOT EXISTS (SELECT NULL
                                                                   FROM   COP_DSP_CONTROLLING_CLOUD.R_PRODUCT_UNIFY
                                                                   WHERE  missing_.CBU_COD     = R_PRODUCT_UNIFY.CBU_COD
                                                                   AND    missing_.PDT_ELM_COD = R_PRODUCT_UNIFY.PRODUCT_CODE)
                                               ) CCD
   ON FLEX.PDT_ELM_KEY = CCD.PDT_ELM_KEY
   WHEN MATCHED THEN 
        UPDATE SET FLEX.PDT_ELM_DSC               = CCD.PDT_ELM_DSC
                  ,FLEX.BRD_HIE_LV1_PTF_BRD_COD   = CCD.BRD_HIE_LV1_PTF_BRD_COD
                  ,FLEX.BRD_HIE_LV1_PTF_BRD_DSC   = CCD.BRD_HIE_LV1_PTF_BRD_DSC
                  ,FLEX.BRD_HIE_LV2_BRD_COD       = CCD.BRD_HIE_LV2_BRD_COD
                  ,FLEX.BRD_HIE_LV2_BRD_DSC       = CCD.BRD_HIE_LV2_BRD_DSC
                  ,FLEX.BRD_HIE_LV3_SUB_BRD_COD   = CCD.BRD_HIE_LV3_SUB_BRD_COD
                  ,FLEX.BRD_HIE_LV3_SUB_BRD_DSC   = CCD.BRD_HIE_LV3_SUB_BRD_DSC
                  ,FLEX.LCL_HIE_LV1_PDT_ARE_COD   = CCD.LCL_HIE_LV1_PDT_ARE_COD
                  ,FLEX.LCL_HIE_LV1_PDT_ARE_DSC   = CCD.LCL_HIE_LV1_PDT_ARE_DSC
                  ,FLEX.LCL_HIE_LV2_UMB_BRD_COD   = CCD.LCL_HIE_LV2_UMB_BRD_COD
                  ,FLEX.LCL_HIE_LV2_UMB_BRD_DSC   = CCD.LCL_HIE_LV2_UMB_BRD_DSC
                  ,FLEX.LCL_HIE_LV3_PDT_BRD_COD   = CCD.LCL_HIE_LV3_PDT_BRD_COD
                  ,FLEX.LCL_HIE_LV3_PDT_BRD_DSC   = CCD.LCL_HIE_LV3_PDT_BRD_DSC
                  ,FLEX.LCL_HIE_LV4_PDT_FAM_COD   = CCD.LCL_HIE_LV4_PDT_FAM_COD
                  ,FLEX.LCL_HIE_LV4_PDT_FAM_DSC   = CCD.LCL_HIE_LV4_PDT_FAM_DSC
                  ,FLEX.LCL_HIE_LV5_PDT_SFM_COD   = CCD.LCL_HIE_LV5_PDT_SFM_COD
                  ,FLEX.LCL_HIE_LV5_PDT_SFM_DSC   = CCD.LCL_HIE_LV5_PDT_SFM_DSC
                  ,FLEX.LCL_HIE_LV6_PDT_NAT_COD   = CCD.LCL_HIE_LV6_PDT_NAT_COD
                  ,FLEX.LCL_HIE_LV6_PDT_NAT_DSC   = CCD.LCL_HIE_LV6_PDT_NAT_DSC
                  ,FLEX.LV0_PDT_CAT_COD           = CCD.LV0_PDT_CAT_COD
                  ,FLEX.LV0_PDT_CAT_DSC           = CCD.LV0_PDT_CAT_DSC
                  ,FLEX.LV1_PDT_ARE_COD           = CCD.LV1_PDT_ARE_COD
                  ,FLEX.LV1_PDT_ARE_DSC           = CCD.LV1_PDT_ARE_DSC
                  ,FLEX.LV2_UMB_BRD_COD           = CCD.LV2_UMB_BRD_COD
                  ,FLEX.LV2_UMB_BRD_DSC           = CCD.LV2_UMB_BRD_DSC
                  ,FLEX.LV3_PDT_BRD_COD           = CCD.LV3_PDT_BRD_COD
                  ,FLEX.LV3_PDT_BRD_DSC           = CCD.LV3_PDT_BRD_DSC
                  ,FLEX.LV4_PDT_FAM_COD           = CCD.LV4_PDT_FAM_COD
                  ,FLEX.LV4_PDT_FAM_DSC           = CCD.LV4_PDT_FAM_DSC
                  ,FLEX.LV5_PDT_SFM_COD           = CCD.LV5_PDT_SFM_COD
                  ,FLEX.LV5_PDT_SFM_DSC           = CCD.LV5_PDT_SFM_DSC
                  ,FLEX.LV6_PDT_NAT_COD           = CCD.LV6_PDT_NAT_COD
                  ,FLEX.LV6_PDT_NAT_DSC           = CCD.LV6_PDT_NAT_DSC
                  ,FLEX.T_REC_DLT_FLG             = CCD.T_REC_DLT_FLG
                  ,FLEX.T_REC_UPD_TST             = CCD.T_REC_UPD_TST
   WHEN NOT MATCHED THEN 
        INSERT (PDT_ELM_KEY
               ,PDT_ELM_COD
               ,PDT_ELM_DSC
               ,CBU_COD

               ,BRD_HIE_LV1_PTF_BRD_COD
               ,BRD_HIE_LV1_PTF_BRD_DSC
               ,BRD_HIE_LV2_BRD_COD
               ,BRD_HIE_LV2_BRD_DSC
               ,BRD_HIE_LV3_SUB_BRD_COD
               ,BRD_HIE_LV3_SUB_BRD_DSC

               ,LCL_HIE_LV1_PDT_ARE_COD
               ,LCL_HIE_LV1_PDT_ARE_DSC
               ,LCL_HIE_LV2_UMB_BRD_COD
               ,LCL_HIE_LV2_UMB_BRD_DSC
               ,LCL_HIE_LV3_PDT_BRD_COD
               ,LCL_HIE_LV3_PDT_BRD_DSC
               ,LCL_HIE_LV4_PDT_FAM_COD
               ,LCL_HIE_LV4_PDT_FAM_DSC
               ,LCL_HIE_LV5_PDT_SFM_COD
               ,LCL_HIE_LV5_PDT_SFM_DSC
               ,LCL_HIE_LV6_PDT_NAT_COD
               ,LCL_HIE_LV6_PDT_NAT_DSC

               ,LV0_PDT_CAT_COD
               ,LV0_PDT_CAT_DSC
               ,LV1_PDT_ARE_COD
               ,LV1_PDT_ARE_DSC
               ,LV2_UMB_BRD_COD
               ,LV2_UMB_BRD_DSC
               ,LV3_PDT_BRD_COD
               ,LV3_PDT_BRD_DSC
               ,LV4_PDT_FAM_COD
               ,LV4_PDT_FAM_DSC
               ,LV5_PDT_SFM_COD
               ,LV5_PDT_SFM_DSC
               ,LV6_PDT_NAT_COD
               ,LV6_PDT_NAT_DSC
               ,T_REC_DLT_FLG
               ,T_REC_INS_TST
               ,T_REC_UPD_TST
               )
        VALUES (CCD.PDT_ELM_KEY
               ,CCD.PDT_ELM_COD
               ,CCD.PDT_ELM_DSC
               ,CCD.CBU_COD

               ,CCD.BRD_HIE_LV1_PTF_BRD_COD
               ,CCD.BRD_HIE_LV1_PTF_BRD_DSC
               ,CCD.BRD_HIE_LV2_BRD_COD
               ,CCD.BRD_HIE_LV2_BRD_DSC
               ,CCD.BRD_HIE_LV3_SUB_BRD_COD
               ,CCD.BRD_HIE_LV3_SUB_BRD_DSC

               ,CCD.LCL_HIE_LV1_PDT_ARE_COD
               ,CCD.LCL_HIE_LV1_PDT_ARE_DSC
               ,CCD.LCL_HIE_LV2_UMB_BRD_COD
               ,CCD.LCL_HIE_LV2_UMB_BRD_DSC
               ,CCD.LCL_HIE_LV3_PDT_BRD_COD
               ,CCD.LCL_HIE_LV3_PDT_BRD_DSC
               ,CCD.LCL_HIE_LV4_PDT_FAM_COD
               ,CCD.LCL_HIE_LV4_PDT_FAM_DSC
               ,CCD.LCL_HIE_LV5_PDT_SFM_COD
               ,CCD.LCL_HIE_LV5_PDT_SFM_DSC
               ,CCD.LCL_HIE_LV6_PDT_NAT_COD
               ,CCD.LCL_HIE_LV6_PDT_NAT_DSC

               ,CCD.LV0_PDT_CAT_COD
               ,CCD.LV0_PDT_CAT_DSC
               ,CCD.LV1_PDT_ARE_COD
               ,CCD.LV1_PDT_ARE_DSC
               ,CCD.LV2_UMB_BRD_COD
               ,CCD.LV2_UMB_BRD_DSC
               ,CCD.LV3_PDT_BRD_COD
               ,CCD.LV3_PDT_BRD_DSC
               ,CCD.LV4_PDT_FAM_COD
               ,CCD.LV4_PDT_FAM_DSC
               ,CCD.LV5_PDT_SFM_COD
               ,CCD.LV5_PDT_SFM_DSC
               ,CCD.LV6_PDT_NAT_COD
               ,CCD.LV6_PDT_NAT_DSC
               ,CCD.T_REC_DLT_FLG
               ,CCD.T_REC_INS_TST
               ,CCD.T_REC_UPD_TST
               )
   ;

   v_STEP_TABLE := 'R_FLX_GRP_PDT';

   TRUNCATE TABLE COP_DMT_FLX.R_FLX_GRP_PDT;

   INSERT OVERWRITE ALL 
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,  -1, '$TOTAL_PDT', 'Total Product', T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,   0, PDT_ELM_COD, PDT_ELM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,   1, LCL_HIE_LV1_PDT_ARE_COD, LCL_HIE_LV1_PDT_ARE_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,   2, LCL_HIE_LV2_UMB_BRD_COD, LCL_HIE_LV2_UMB_BRD_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,   3, LCL_HIE_LV3_PDT_BRD_COD, LCL_HIE_LV3_PDT_BRD_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,   4, LCL_HIE_LV4_PDT_FAM_COD, LCL_HIE_LV4_PDT_FAM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,   5, LCL_HIE_LV5_PDT_SFM_COD, LCL_HIE_LV5_PDT_SFM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,   6, LCL_HIE_LV6_PDT_NAT_COD, LCL_HIE_LV6_PDT_NAT_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,  10, LV0_PDT_CAT_COD, LV0_PDT_CAT_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,  11, LV1_PDT_ARE_COD, LV1_PDT_ARE_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,  12, LV2_UMB_BRD_COD, LV2_UMB_BRD_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,  13, LV3_PDT_BRD_COD, LV3_PDT_BRD_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,  14, LV4_PDT_FAM_COD, LV4_PDT_FAM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,  15, LV5_PDT_SFM_COD, LV5_PDT_SFM_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD,  16, LV6_PDT_NAT_COD, LV6_PDT_NAT_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD, 101, BRD_HIE_LV1_PTF_BRD_COD, BRD_HIE_LV1_PTF_BRD_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD, 102, BRD_HIE_LV2_BRD_COD, BRD_HIE_LV2_BRD_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)
       INTO COP_DMT_FLX.R_FLX_GRP_PDT(CBU_COD,PDT_ELM_COD,PDT_DIM_GRP_COD,PDT_GRP_COD,PDT_GRP_DSC,T_REC_DLT_FLG,T_REC_INS_TST,T_REC_UPD_TST)
            VALUES (CBU_COD, PDT_ELM_COD, 103, BRD_HIE_LV3_SUB_BRD_COD, BRD_HIE_LV3_SUB_BRD_DSC, T_REC_DLT_FLG, T_REC_INS_TST, T_REC_UPD_TST)

   SELECT   pdt.CBU_COD
           ,pdt.PDT_ELM_COD
           ,pdt.PDT_ELM_DSC
           ,pdt.BRD_HIE_LV1_PTF_BRD_COD
           ,pdt.BRD_HIE_LV1_PTF_BRD_DSC
           ,pdt.BRD_HIE_LV2_BRD_COD
           ,pdt.BRD_HIE_LV2_BRD_DSC
           ,pdt.BRD_HIE_LV3_SUB_BRD_COD
           ,pdt.BRD_HIE_LV3_SUB_BRD_DSC
           ,pdt.LCL_HIE_LV1_PDT_ARE_COD
           ,pdt.LCL_HIE_LV1_PDT_ARE_DSC
           ,pdt.LCL_HIE_LV2_UMB_BRD_COD
           ,pdt.LCL_HIE_LV2_UMB_BRD_DSC
           ,pdt.LCL_HIE_LV3_PDT_BRD_COD
           ,pdt.LCL_HIE_LV3_PDT_BRD_DSC
           ,pdt.LCL_HIE_LV4_PDT_FAM_COD
           ,pdt.LCL_HIE_LV4_PDT_FAM_DSC
           ,pdt.LCL_HIE_LV5_PDT_SFM_COD
           ,pdt.LCL_HIE_LV5_PDT_SFM_DSC
           ,pdt.LCL_HIE_LV6_PDT_NAT_COD
           ,pdt.LCL_HIE_LV6_PDT_NAT_DSC
           ,pdt.LV0_PDT_CAT_COD
           ,pdt.LV0_PDT_CAT_DSC
           ,pdt.LV1_PDT_ARE_COD
           ,pdt.LV1_PDT_ARE_DSC
           ,pdt.LV2_UMB_BRD_COD
           ,pdt.LV2_UMB_BRD_DSC
           ,pdt.LV3_PDT_BRD_COD
           ,pdt.LV3_PDT_BRD_DSC
           ,pdt.LV4_PDT_FAM_COD
           ,pdt.LV4_PDT_FAM_DSC
           ,pdt.LV5_PDT_SFM_COD
           ,pdt.LV5_PDT_SFM_DSC
           ,pdt.LV6_PDT_NAT_COD
           ,pdt.LV6_PDT_NAT_DSC
           ,0                            AS T_REC_DLT_FLG
           ,current_timestamp            as T_REC_INS_TST
           ,current_timestamp            as T_REC_UPD_TST
   FROM     COP_DMT_FLX.R_FLX_PDT as pdt
   WHERE pdt.T_REC_DLT_FLG = 0
   ORDER BY CBU_COD, PDT_ELM_COD;

   -- Return text when the stored procedure completes
   RETURN 'Success';

EXCEPTION
   WHEN OTHER THEN
        RETURN SQLCODE || ': ' || SQLERRM || ' in the step ' || v_STEP_TABLE;
        RAISE; -- Raise the same exception that you are handling.
END;
$$
;
