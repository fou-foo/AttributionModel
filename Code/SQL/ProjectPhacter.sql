CREATE OR REPLACE TABLE `ga360-250517.cenic.compras_kiosco_cool` as (
  SELECT *
    FROM `ga360-250517.cenic.compras_kiosco` as K
    INNER JOIN `ga360-250517.cenic.PurchaseWEB` Web
  on CAST( Web.transactionId as int64)  =K.sec_ordencommerce )


  