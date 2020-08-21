## Sessions
WITH rawdata AS ( SELECT DISTINCT
  PARSE_DATE('%Y%m%d',date) AS date,
  fullvisitorId,
  CONCAT(fullVisitorId,'.',CAST(visitStartTime AS STRING)) AS sessionId,
  visitStartTime,
  UPPER(trafficSource.source) AS source, 
  UPPER(trafficSource.medium) AS medium,
  trafficSource.campaign,
  device.deviceCategory AS dispositivo,
  device.operatingSystem AS sistemaOperativo,
  device.browser AS navegador,
  geoNetwork.region,
  geoNetwork.city, 
FROM `ga360-250517.53461765.ga_sessions_*`, UNNEST(hits) AS hits
WHERE _TABLE_SUFFIX = '20200720'  --AND FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE("America/Mazatlan"), INTERVAL 1 DAY))
)
SELECT
  date, sessionId,region,city, sistemaOperativo,navegador,
  CASE
    WHEN des_buildingBlockAgg IS NULL AND medium = 'REFERRAL' THEN 'SEO & OWNED MEDIA'
    WHEN des_buildingBlockAgg IS NULL AND medium != 'REFERRAL' THEN 'OTROS INGRESOS'
    ELSE des_buildingBlockAgg
  END AS buildingBlockAgg,
  CASE
    WHEN des_buildingBlockAgg IS NULL AND UPPER(medium) = 'REFERRAL' THEN 'REFERRAL'
    WHEN des_buildingBlockAgg IS NULL AND UPPER(medium) != 'REFERRAL' THEN 'NO IDENTIFICADOS'
    ELSE des_buildingBlock
  END AS buildingBlock, dispositivo,
FROM rawData
LEFT JOIN
(SELECT * FROM `ga360-250517.dataProcessed.buildingBlocks`) BBs
ON rawData.source = BBs.des_fuente AND rawData.medium = BBs.des_medio
GROUP BY 1,2,3,4,5,6,7,8,9

--------------------------------------------------------------------------

## SessionPurchase
WITH rawdata AS ( SELECT DISTINCT
  PARSE_DATE('%Y%m%d',date) AS date,
  fullvisitorId,
  CONCAT(fullVisitorId,'.',CAST(visitStartTime AS STRING)) AS sessionId,
  UPPER(trafficSource.source) AS source, 
  UPPER(trafficSource.medium) AS medium,
  trafficSource.campaign,
  device.deviceCategory AS dispositivo,
  
FROM `ga360-250517.53461765.ga_sessions_*`, UNNEST(hits) AS hits
WHERE _TABLE_SUFFIX = '20200720'  --AND FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE("America/Mazatlan"), INTERVAL 1 DAY))
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
        WHERE _TABLE_SUFFIX = '20200720' AND hits.transaction.transactionId IS NOT null-- '20200804' AND 
        ORDER BY hitNumber, sessionId, transactionId
    )
  )
WHERE firstHit = 1
)

 SELECT DISTINCT A.*,time, transactionId, transactionRevenue FROM
 (SELECT * FROM rawdata) A
 LEFT JOIN
 (SELECT * FROM transacciones) B
 ON A.sessionId = B.sessionId
 WHERE transactionId IS NOT null