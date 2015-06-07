SELECT IDS_MF.BUSINESS_DIVISION_CODE                                              ,
  INITCAP(IDS_MF.BUSINESS_DIVISION_DESCR)AS BusinessDivisionDescr                  ,
  (IDS_MF.BUSINESS_DIVISION_CODE
  ||'--'
  ||INITCAP(IDS_MF.BUSINESS_DIVISION_DESCR))AS BusinessDivisionCodeDescr           ,
  IDS_MF.REGION_CODE                                                               ,
  IDS_MF.REGION_DESCR                                                              ,
  IDS_MF.MINOR_LINE_CODE                                                           ,
  INITCAP(IDS_MF.MINOR_LINE_DESCR) AS MINOR_LINE_DESCR                             ,
  IDS_MF.YEAR_DESCR                                                                ,
  IDS_MF.MONTH_DESCR                                                               ,
  SUM(IDS_MF.MF_Pounds_Shipped     + IDS_MF.IDS_Pounds_Shipped)                                                                                               AS TOTAL_POUNDS_SHIPPED,
  SUM(IDS_MF.MF_Gross_Sales_Amount - IDS_MF.MF_Allow_Amount - IDS_MF.MF_Off_Invoice_Amount + IDS_MF.MF_Revenue_Adjustment + IDS_MF.IDS_Indirect_Sales_Amount) AS NET_RECEIVABLE_DOLLAR
   FROM
  (
  /*Begin Sub query*/
   SELECT MPBD1.BUSINESS_DIVISION_CODE                  ,
    MPBD1.BUSINESS_DIVISION_DESCR                       ,
    M1.REGION_CODE                                      ,
    M1.REGION_DESCR                                     ,
    P1.MINOR_LINE_CODE                                  ,
    P1.MINOR_LINE_DESCR                                 ,
    WD.YEAR_DESCR                                       ,
    WD.MONTH_DESCR                                      ,
    MF.POUNDS_SHIPPED     AS MF_Pounds_Shipped     ,
    MF.GROSS_SALES_AMOUNT AS MF_Gross_Sales_Amount ,
    MF.ALLOW_AMOUNT   AS MF_Allow_Amount       ,
    mf.gross_sales_variance AS MF_Revenue_Adjustment ,
    MF.OFF_INVOICE_AMOUNT AS MF_Off_Invoice_Amount ,
    0                            AS IDS_Pounds_Shipped    ,
    0                            AS IDS_Indirect_Sales_Amount
     FROM TDW_OWNER.RESTATED_MKT_PRD_BUS_DIV_DIM MPBD1 ,
    TDW_OWNER.RESTATED_MARKET M1                       ,
    TDW_OWNER.RESTATED_PRODUCT P1                      ,
    tdw_owner.ta_margin_fact_view mf                   ,
    tdw_owner.week_dim wd
    WHERE MF.RESTATED_MARKET_KEY      = M1.RESTATED_MARKET_KEY
  AND MF.RESTATED_PRODUCT_KEY         = P1.RESTATED_PRODUCT_KEY
  AND MF.RESTATED_MARKET_KEY          = MPBD1.RESTATED_MARKET_KEY
  AND MF.RESTATED_PRODUCT_KEY         = MPBD1.RESTATED_PRODUCT_KEY
  AND To_DATE(mf.week_key,'YYYYMMDD') = wd.week_ending_date
  AND(MPBD1.BUSINESS_DIVISION_CODE IN ('F01', 'F03','F04', 'F08', 'F11', 'F12', 'F15'))
  AND (M1.SELLING_GROUP_CODE       IN ('119000','125000','610000','800000','423000'))
  AND (wd.YEAR_DESCR               IN ('FY2009'))
  AND wd.FISCAL_MONTH_NUMBER <= 12
  AND (MPBD1.BUSINESS_DIVISION_CODE IN('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15'))
  AND(M1.DIVISION_CODE              IN('102','141'))
  AND (M1.REGION_CODE               IN('137','169'))
  AND (M1.PRIMARY_BROKER_CODE       IN('321','544'))
                

    UNION ALL
   
   SELECT
    MPBD1.BUSINESS_DIVISION_CODE                 ,
    MPBD1.BUSINESS_DIVISION_DESCR                ,
    M1.REGION_CODE                               ,
    M1.REGION_DESCR                              ,
    P1.MINOR_LINE_CODE                           ,
    P1.MINOR_LINE_DESCR                          ,
    MD.YEAR_DESCR                                ,
    MD.MONTH_DESCR                               ,
    0 AS MF_Pounds_Shipped                       ,
    0 AS MF_Gross_Sales_Amount                   ,
    0 AS MF_Allow_Amount                         ,
    0 AS MF_Revenue_Adjustment                   ,
    0 AS MF_Off_Invoice_Amount                   ,
    IDS.POUNDS_SHIPPED AS IDS_Pounds_Shipped ,
    IDS.INDIRECT_SALES_AMOUNT AS IDS_Indirect_Sales_Amount
     FROM TDW_OWNER.RESTATED_MKT_PRD_BUS_DIV_DIM MPBD1 ,
    TDW_OWNER.RESTATED_MARKET M1                       ,
    TDW_OWNER.MONTH_DIM MD                             ,
    TDW_OWNER.RESTATED_PRODUCT P1                      ,
    TDW_OWNER.INDIRECT_SALES IDS                       ,
    TDW_OWNER.WEEK_DIM W
    WHERE IDS.RESTATED_MARKET_KEY    = M1.RESTATED_MARKET_KEY
  AND IDS.RESTATED_PRODUCT_KEY       = P1.RESTATED_PRODUCT_KEY
  AND IDS.RESTATED_MARKET_KEY        = MPBD1.RESTATED_MARKET_KEY
  AND IDS.RESTATED_PRODUCT_KEY       = MPBD1.RESTATED_PRODUCT_KEY
  AND IDS.WEEK_KEY                   = W.WEEK_KEY
  AND W.MONTH_ENDING_DATE            = MD.MONTH_ENDING_DATE
  AND(MPBD1.BUSINESS_DIVISION_CODE  IN ('F01', 'F03', 'F04', 'F08', 'F11', 'F12', 'F15'))
  AND (M1.SELLING_GROUP_CODE        IN ('119000','125000','610000','800000','423000'))
  AND (MD.YEAR_DESCR                IN('FY2009'))
  AND (MD.FISCAL_MONTH_NUMBER        = (12))
  AND (MPBD1.BUSINESS_DIVISION_CODE IN('F01','F03','F04','F08','F11','F12','F15'))
  AND (M1.DIVISION_CODE             IN('102','141'))
  AND (M1.REGION_CODE               IN('137','169'))
  AND (M1.PRIMARY_BROKER_CODE       IN('321','544'))
  )
  /*end sub query*/
  IDS_MF
GROUP BY IDS_MF.BUSINESS_DIVISION_CODE   ,
  INITCAP(IDS_MF.BUSINESS_DIVISION_DESCR),
  IDS_MF.REGION_CODE                        ,
  IDS_MF.REGION_DESCR                       ,
  IDS_MF.MINOR_LINE_CODE                    ,
  INITCAP(IDS_MF.MINOR_LINE_DESCR)          ,
  IDS_MF.YEAR_DESCR                    ,
  IDS_MF.MONTH_DESCR
ORDER BY BusinessDivisionDescr,
IDS_MF.MINOR_LINE_CODE
