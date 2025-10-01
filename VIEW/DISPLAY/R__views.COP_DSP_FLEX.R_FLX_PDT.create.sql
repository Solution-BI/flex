USE SCHEMA COP_DSP_FLEX;

CREATE OR REPLACE VIEW R_FLX_PDT 
         (PDT_ELM_KEY                                                 COMMENT 'Product Key : concatenation of CBU and Product Code'
         ,PDT_ELM_COD                                                 COMMENT 'Product code'
         ,PDT_ELM_DSC                                                 COMMENT 'Product name'
         ,CBU_COD                                                     COMMENT 'CBU/Market'
         ,BRD_HIE_LV1_PTF_BRD_COD                                     COMMENT 'Plateform code in the Brand Hierarchy (Level 1)'
         ,BRD_HIE_LV1_PTF_BRD_DSC                                     COMMENT 'Plateform name in the Brand Hierarchy (Level 1)'
         ,BRD_HIE_LV2_BRD_COD                                         COMMENT 'Brand code in the Brand Hierarchy (Level 2'
         ,BRD_HIE_LV2_BRD_DSC                                         COMMENT 'Brand name in the Brand Hierarchy (Level 2'
         ,BRD_HIE_LV3_SUB_BRD_COD                                     COMMENT 'Sub-Brand code in the Brand Hierarchy (Level 3)'
         ,BRD_HIE_LV3_SUB_BRD_DSC                                     COMMENT 'Sub-Brand name in the Brand Hierarchy (Level 3)'
         ,LCL_HIE_LV1_PDT_ARE_COD                                     COMMENT 'Product Area code in the Local Hierarchy (Level 1)'
         ,LCL_HIE_LV1_PDT_ARE_DSC                                     COMMENT 'Product Area name in the Local  Hierarchy (Level 1)'
         ,LCL_HIE_LV2_UMB_BRD_COD                                     COMMENT 'Umbrella Brand code in the Local  Hierarchy (Level 2)'
         ,LCL_HIE_LV2_UMB_BRD_DSC                                     COMMENT 'Umbrella Brand name in the Local  Hierarchy (Level 2)'
         ,LCL_HIE_LV3_PDT_BRD_COD                                     COMMENT 'Product Brand code in the Local  Hierarchy (Level 3)'
         ,LCL_HIE_LV3_PDT_BRD_DSC                                     COMMENT 'Product Brand name in the Local  Hierarchy (Level 3)'
         ,LCL_HIE_LV4_PDT_FAM_COD                                     COMMENT 'Product Family code in the Local  Hierarchy (Level 4)'
         ,LCL_HIE_LV4_PDT_FAM_DSC                                     COMMENT 'Product Family name in the Local  Hierarchy (Level 4)'
         ,LCL_HIE_LV5_PDT_SFM_COD                                     COMMENT 'Product Sub-Family code in the Local  Hierarchy (Level 5)'
         ,LCL_HIE_LV5_PDT_SFM_DSC                                     COMMENT 'Product Sub-Family name in the Local  Hierarchy (Level 5)'
         ,LCL_HIE_LV6_PDT_NAT_COD                                     COMMENT 'Product Nature code in the Local  Hierarchy (Level 6)'
         ,LCL_HIE_LV6_PDT_NAT_DSC                                     COMMENT 'Product Nature name in the Local  Hierarchy (Level 6)'
         ,LV0_PDT_CAT_COD                                             COMMENT 'Product Category code in the Global Hierarchy (Level 0)'
         ,LV0_PDT_CAT_DSC                                             COMMENT 'Product Category name in the Global Hierarchy (Level 0)'
         ,LV1_PDT_ARE_COD                                             COMMENT 'Product Area code in the Global Hierarchy (Level 1)'
         ,LV1_PDT_ARE_DSC                                             COMMENT 'Product Area name in the Global Hierarchy (Level 1)'
         ,LV2_UMB_BRD_COD                                             COMMENT 'Umbrella Brand code in the Global Hierarchy (Level 1)'
         ,LV2_UMB_BRD_DSC                                             COMMENT 'Umbrella Brand name in the Global Hierarchy (Level 2)'
         ,LV3_PDT_BRD_COD                                             COMMENT 'Product Brand code in the Global Hierarchy (Level 3)'
         ,LV3_PDT_BRD_DSC                                             COMMENT 'Product Brand name in the Global Hierarchy (Level 3)'
         ,LV4_PDT_FAM_COD                                             COMMENT 'Product Family code in the Global Hierarchy (Level 4)'
         ,LV4_PDT_FAM_DSC                                             COMMENT 'Product Family name in the Global Hierarchy (Level 4)'
         ,LV5_PDT_SFM_COD                                             COMMENT 'Product Sub-Family code in the Global Hierarchy (Level 5)'
         ,LV5_PDT_SFM_DSC                                             COMMENT 'Product Sub-Family name in the Global Hierarchy (Level 5)'
         ,LV6_PDT_NAT_COD                                             COMMENT 'Product Nature code in the Global Hierarchy (Level 6)'
         ,LV6_PDT_NAT_DSC                                             COMMENT 'Product Nature name in the Global Hierarchy (Level 6)'
         ) COMMENT = '[Flex] Product masterdata'
AS
SELECT    PDT_ELM_KEY
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
FROM      COP_DMT_FLX.R_FLX_PDT
;
