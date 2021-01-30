-- Generacion de las 3 tablas de insumo para el proyecto 
-- Modelo de atribución: ventas de Quiosco producidas por Paid Media  (CINV-MAPM-MX-D-0002)
-- ecommerce: jesus.angulo@coppel.com, ivan.garcia@coppel.com; CENIC: antonio.garciar@coppel.com, fernando.villa@coppel.com

/* 
Se obtienen todas las sesiones del sitio web de coppel.com 
*/ 

CREATE OR REPLACE TABLE ga360-250517.cenic.SessionsWEB as (
WITH rawdata AS (
SELECT DISTINCT
    visitStartTime,
    #PARSE_DATE('%Y%m%d',date) AS date,
    #geoNetwork.country,
    CONCAT(fullVisitorId,'.',CAST(visitStartTime AS STRING)) AS sessionId,
    fullvisitorId, -- identificador de Google Analytics 
    UPPER(trafficSource.medium) AS medium, -- medio asignado por GA
    UPPER(trafficSource.source) AS source,
    trafficSource.campaign,
    device.operatingSystem,
    device.deviceCategory,
    device.browser,
    geoNetwork.region,
    geoNetwork.city,
    hits.time,
FROM `ga360-250517.53461765.ga_sessions_*`, UNNEST(hits) AS hits
WHERE #_TABLE_SUFFIX = '20200720'  AND
 geoNetwork.country='Mexico' -- filtramos solo las sessiones de Mexico
)

/*
Para determinar el Building Block que la gerencia de BI de ecommerce asigna a cada session se realiza el siguiene cruce (lo realizo Ivan)
*/
SELECT * from (
SELECT
  rawdata.*,
  CASE
    WHEN des_buildingBlockAgg IS NULL AND medium = 'REFERRAL' THEN 'SEO & OWNED MEDIA'
    WHEN des_buildingBlockAgg IS NULL AND medium != 'REFERRAL' THEN 'OTROS INGRESOS'
    ELSE des_buildingBlockAgg
  END AS buildingBlockAgg,
  CASE
    WHEN des_buildingBlockAgg IS NULL AND UPPER(medium) = 'REFERRAL' THEN 'REFERRAL'
    WHEN des_buildingBlockAgg IS NULL AND UPPER(medium) != 'REFERRAL' THEN 'NO IDENTIFICADOS'
    ELSE des_buildingBlock
  END AS buildingBlock,
FROM rawData
LEFT JOIN (
    SELECT *
        FROM `ga360-250517.cenic.buildingBlocks`) BBs -- esta tabla esta en un proecto de la gerencia de BI pero Jesus la copio al dataset del proyecto 'cenic'
  ON rawData.source = BBs.des_fuente AND rawData.medium = BBs.des_medio
GROUP BY 1,2,3,4,5,6,7,8, 9, 10, 11,12, 13,14 ) as temporal1

)

/* 
Se obtienen todas las sesiones CON VENTA del sitio web de coppel.com 
*/ 

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
#WHERE _TABLE_SUFFIX = '20200720'
),

/* Esta seccion es para evitar contar mas de una vez una venta debido al page de 'gracias por su compra' al finalizar la venta*/
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
        #_TABLE_SUFFIX = '20200720' AND
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


/* Conjunto de ventas en el kiosko identificables en Mexico */
/* Ya se consideran los usuarios que compraron en el kiosko y que tambien compraron en el sitio web  */
CREATE OR REPLACE TABLE `ga360-250517.cenic.compras_kiosco_cool` as (
  SELECT *
    FROM `ga360-250517.cenic.compras_kiosco` as K
    INNER JOIN `ga360-250517.cenic.PurchaseWEB` Web
  on CAST( Web.transactionId as int64)  =K.sec_ordencommerce )


/* Conjunto de sessiones en el sitio web de los clientes del kiosko identificables en Mexico */
CREATE OR REPLACE TABLE `ga360-250517.cenic.SessionsKiosk` As (
SELECT *  
  FROM  `ga360-250517.cenic.SessionsWEB` 
  WHERE fullvisitorId IN ( SELECT fullvisitorId FROM `ga360-250517.cenic.compras_kiosco_cool`) 
  )

    /* QUERY PARA VALIDAR MONTOS CONTRA EL DASHBOARD OMNICANAL */
SELECT extract (year from fecha_inicio ) as anio, fuente, sum(gasto) as gasto 
FROM `ga360-250517.cenic.mae_rendimientoCampanias` 
WHERE fecha_inicio is not null 
group by anio, fuente
order by anio desc , fuente 
