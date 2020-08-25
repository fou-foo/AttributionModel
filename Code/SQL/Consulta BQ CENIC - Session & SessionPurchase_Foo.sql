##############################################################
## Generacion de las tablas de insumo para el proyecto
## Modelo de atribución: ventas de Quiosco producidas por Paid Media 
## (CINV-MAPM-MX-D-0002)
## CENIC:Script tipeado en su gran mayoria con la valiosa ayuda de jesus.angulo@coppel.com y 
## 
##############################################################

## Sessions

CREATE OR REPLACE TABLE ga360-250517.cenic.SessionsWEB as (
## Sessions
WITH rawdata AS (
SELECT DISTINCT
    #PARSE_DATE('%Y%m%d',date) AS date,
    #geoNetwork.country,
    CONCAT(fullVisitorId,'.',CAST(visitStartTime AS STRING)) AS sessionId,
    fullvisitorId,
    UPPER(trafficSource.medium) AS medium,
    UPPER(trafficSource.source) AS source,
    trafficSource.campaign,
    device.operatingSystem,
    device.deviceCategory,
    device.browser,
    geoNetwork.region,
    geoNetwork.city,
    visitStartTime,
FROM `ga360-250517.53461765.ga_sessions_*`, UNNEST(hits) AS hits
WHERE _TABLE_SUFFIX = '20200720'  --AND FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE("America/Mazatlan"), INTERVAL 1 DAY))
# AND geoNetwork.country='Mexico'
)

SELECT * from (
SELECT
  sessionId, region, city, operatingSystem, browser,
  CASE
    WHEN des_buildingBlockAgg IS NULL AND medium = 'REFERRAL' THEN 'SEO & OWNED MEDIA'
    WHEN des_buildingBlockAgg IS NULL AND medium != 'REFERRAL' THEN 'OTROS INGRESOS'
    ELSE des_buildingBlockAgg
  END AS buildingBlockAgg,
  CASE
    WHEN des_buildingBlockAgg IS NULL AND UPPER(medium) = 'REFERRAL' THEN 'REFERRAL'
    WHEN des_buildingBlockAgg IS NULL AND UPPER(medium) != 'REFERRAL' THEN 'NO IDENTIFICADOS'
    ELSE des_buildingBlock
  END AS buildingBlock, deviceCategory,
FROM rawData
LEFT JOIN (
    SELECT *
        FROM `ga360-250517.cenic.buildingBlocks`) BBs
  ON rawData.source = BBs.des_fuente AND rawData.medium = BBs.des_medio
GROUP BY 1,2,3,4,5,6,7,8  ) as temporal1
)
--------------------------------------------------------------------------

## SessionPurchase
CREATE OR REPLACE TABLE  `ga360-250517.cenic.PurchaseWEB` AS (
WITH rawdata AS ( SELECT DISTINCT
  visitStartTime,
  #PARSE_DATE('%Y%m%d',date) AS date,
  fullvisitorId,
  CONCAT(fullVisitorId,'.',CAST(visitStartTime AS STRING)) AS sessionId,
  UPPER(trafficSource.source) AS source,
  UPPER(trafficSource.medium) AS medium,
  trafficSource.campaign,
  device.deviceCategory AS dispositivo

FROM `ga360-250517.53461765.ga_sessions_*`, UNNEST(hits) AS hits
WHERE _TABLE_SUFFIX = '20200720'
),

transacciones AS (
SELECT DISTINCT
  sessionId,
  hitNumber,
  time,
  transactionId, transactionRevenue
FROM
  (
    SELECT
      sessionId,
      hitNumber,
      time,
      RANK() OVER (PARTITION BY sessionId ORDER BY hitNumber ASC) AS firstHit,
      transactionId, transactionRevenue
    FROM
    (
        SELECT DISTINCT
          date,
          hits.hitNumber,
          hits.time,
          CONCAT(fullVisitorId,".",CAST(visitStartTime AS STRING)) AS sessionId,
          hits.transaction.transactionId, hits.transaction.transactionRevenue/1000000 AS transactionRevenue
        FROM `ga360-250517.53461765.ga_sessions_*`, UNNEST(hits) AS hits, UNNEST(hits.product) AS product
        WHERE
        _TABLE_SUFFIX = '20200720' AND
        hits.transaction.transactionId IS NOT null
        ORDER BY hitNumber, sessionId, transactionId
    )
  )
WHERE firstHit = 1
)

SELECT * from (
 SELECT DISTINCT A.*,time, transactionId, transactionRevenue FROM
 (SELECT * FROM rawdata) A
 LEFT JOIN
 (SELECT * FROM transacciones) B
 ON A.sessionId = B.sessionId
 WHERE transactionId IS NOT null  ) AS temporal
)
