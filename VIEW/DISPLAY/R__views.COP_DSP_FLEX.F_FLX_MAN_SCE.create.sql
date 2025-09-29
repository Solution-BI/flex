USE SCHEMA COP_DSP_FLEX{{uid}};

CREATE OR REPLACE VIEW F_FLX_MAN_SCE 
         (ID
         ,MAN_SCE_ELM_KEY
         ,MAN_SCE_ELM_COD
         ,CBU_COD
         ,IND_ELM_USR_DSC
         ,IND_ELM_COD
         ,IND_ELM_DSC
         ,PER_ELM_COD
         ,PER_ELM_DSC
         ,ETI_ELM_COD
         ,ETI_ELM_DSC
         ,CUS_ELM_COD
         ,CUS_ELM_DSC
         ,PDT_ELM_COD
         ,PDT_ELM_DSC
         ,CAT_TYP_ELM_COD
         ,CAT_TYP_ELM_DSC
         ,EIB_ELM_COD
         ,EIB_ELM_DSC
         ,TTY_ELM_COD
         ,TTY_ELM_DSC
         ,ACC_ELM_COD
         ,ACC_ELM_DSC
         ,DST_ELM_COD
         ,DST_ELM_DSC
         ,FCT_ARE_ELM_COD
         ,FCT_ARE_ELM_DSC
         ,AMOUNT
         ,MAN_ITM_ERR_DTA
         ,MAN_ITM_ERR_MSG
         ,MAN_ITM_ERR_FLG
         ) COMMENT = '[Flex] Manual dataset input table'
AS
SELECT    sce.ID
         ,sce.MAN_SCE_ELM_KEY
         ,sce.MAN_SCE_ELM_COD
         ,sce.CBU_COD
         ,sce.IND_ELM_USR_DSC
         ,sce.IND_ELM_COD
         ,COALESCE(ind.IND_ELM_DSC,'#Error: No base indicator identified from the configuration')   IND_ELM_DSC
         ,sce.PER_ELM_COD
         ,COALESCE(per.PER_ELM_DSC,'#Error: Period not found in masterdata')                        PER_ELM_DSC
         ,sce.ETI_ELM_COD
         ,COALESCE(eti.ETI_ELM_DSC,'#Error: Entity not found in masterdata')                        ETI_ELM_DSC
         ,sce.CUS_ELM_COD
         ,COALESCE(cus.CUS_ELM_DSC,'#Error: Customer not found in masterdata')                      CUS_ELM_DSC
         ,sce.PDT_ELM_COD
         ,COALESCE(pdt.PDT_ELM_DSC,'#Error: Product not found in masterdata')                       PDT_ELM_DSC
         ,sce.CAT_TYP_ELM_COD
         ,COALESCE(ct.CAT_TYP_ELM_DSC,'#Error: Managerial/Interco config  not found in masterdata') CAT_TYP_ELM_DSC
         ,sce.EIB_ELM_COD
         ,COALESCE(eib.EIB_ELM_DSC,'#Error: EIB not found in masterdata')                           EIB_ELM_DSC
         ,sce.TTY_ELM_COD
         ,COALESCE(tty.TTY_ELM_DSC,'#Error: Territory not found in masterdata')                     TTY_ELM_DSC
         ,sce.ACC_ELM_COD
         ,COALESCE(acc.ACC_ELM_DSC,'#Error: Account not found in masterdata')                       ACC_ELM_DSC
         ,sce.DST_ELM_COD
         ,COALESCE(dst.DST_ELM_DSC,'#Error: Destination not found in masterdata')                   DST_ELM_DSC
         ,sce.FCT_ARE_ELM_COD
         ,COALESCE(fa.FCT_ARE_ELM_DSC,'#Error: Functional Area not found in masterdata')            FCT_ARE_ELM_DSC
         ,sce.AMOUNT
         ,sce.MAN_ITM_ERR_DTA
         ,RTRIM((CASE WHEN sce.IND_ELM_COD     = '#ERR' THEN 'Indicator ''' || IND_ELM_USR_DSC || ''' is not a base indicator\\n' ELSE '' END) ||
                (CASE WHEN ACC_ERR_FLG         = 1      THEN 'Account ''' || sce.ACC_ELM_COD || ''' does not exist in the masterdata\\n' ELSE '' END) ||
                (CASE WHEN DST_ERR_FLG         = 1      THEN 'Destination ''' || sce.DST_ELM_COD || ''' does not exist in the masterdata\\n' ELSE '' END) ||
                (CASE WHEN FCT_ARE_ERR_FLG     = 1      THEN 'Functional Area ''' || sce.FCT_ARE_ELM_COD || ''' does not exist in the masterdata\\n' ELSE '' END) ||
                (CASE WHEN CUS_ERR_FLG         = 1      THEN 'Customer ''' || sce.CUS_ELM_COD || ''' does not exist in the masterdata\\n' ELSE '' END) ||
                (CASE WHEN PDT_ERR_FLG         = 1      THEN 'Product ''' || sce.PDT_ELM_COD || ''' does not exist in the masterdata\\n' ELSE '' END) ||
                (CASE WHEN ETI_ERR_FLG         = 1      THEN 'Entity ''' || sce.ETI_ELM_COD || ''' does not exist in the masterdata\\n' ELSE '' END) ||
                (CASE WHEN EIB_ERR_FLG         = 1      THEN 'EIB ''' || sce.EIB_ELM_COD || ''' does not exist in the masterdata\\n' ELSE '' END) ||
                (CASE WHEN TTY_ERR_FLG         = 1      THEN 'Territory ''' || sce.TTY_ELM_COD || ''' does not exist in the masterdata\\n' ELSE '' END) ||
                (CASE WHEN CAT_TYP_ERR_FLG     = 1      THEN 'Managerial/Interco config ''' || sce.CAT_TYP_ELM_COD || ''' does not exist in the masterdata\\n' ELSE '' END) ||
                (CASE WHEN PER_ERR_FLG         = 1      THEN 'Period ''' || sce.PER_ELM_COD || ''' must be between 01 and 12,' ELSE '' END),'\\n')    MAN_ITM_ERR_MSG
         ,sce.MAN_ITM_ERR_FLG
FROM      COP_DMT_FLX.F_FLX_MAN_SCE sce
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_IND AS ind on 
          (
              sce.IND_ELM_COD = ind.IND_ELM_COD
          )
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_PER AS per on 
          (
             sce.PER_ELM_COD = per.PER_ELM_COD
          )
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_ETI AS eti on 
          (
             sce.cbu_cod || '-' || sce.ETI_ELM_COD = eti.ETI_ELM_KEY
          )
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_CUS AS cus on 
          (
             sce.cbu_cod || '-' || sce.CUS_ELM_COD = cus.CUS_ELM_KEY
          )
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_PDT AS pdt on 
          (
             sce.cbu_cod || '-' || sce.PDT_ELM_COD = pdt.PDT_ELM_KEY
          )
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_EIB AS eib on 
          (
             sce.cbu_cod || '-' || sce.EIB_ELM_COD = eib.EIB_ELM_KEY
          )
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_TTY AS tty on 
          (
             sce.cbu_cod || '-' || sce.TTY_ELM_COD = tty.TTY_ELM_KEY
          )
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_CAT_TYP AS ct on 
          (
             sce.cbu_cod || '-' || sce.CAT_TYP_ELM_COD = ct.CAT_TYP_ELM_KEY
          )
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_ACCOUNT AS acc on 
          (
             sce.cbu_cod     = acc.cbu_cod     AND
             sce.ACC_ELM_COD = acc.ACC_ELM_COD
          )
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_DESTINATION AS dst on 
          (
             sce.cbu_cod     = dst.cbu_cod     AND
             sce.DST_ELM_COD = dst.DST_ELM_KEY
          )
          LEFT OUTER JOIN COP_DMT_FLX.R_FLX_FCT_ARE AS fa on 
          (
             sce.cbu_cod         = fa.cbu_cod         AND
             sce.FCT_ARE_ELM_COD = fa.FCT_ARE_ELM_COD
          )
WHERE     sce.T_REC_DLT_FLG = 0
;
