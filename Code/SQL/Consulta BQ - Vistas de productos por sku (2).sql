WITH rawData AS
(SELECT
  date,
  fullVisitorId,
  CONCAT(fullVisitorId,'.',CAST(visitStartTime AS STRING)) AS sessionId,
  geoNetwork.region,
  geoNetwork.city,
  trafficSource.campaign,
  trafficSource.source,
  trafficSource.medium,
  REGEXP_EXTRACT(hits.page.pagePath,r'[Pp][MmRr]-([0-9]{7})') AS sku,
  REGEXP_EXTRACT(hits.page.pageTitle,r'(.*) [0-9]{7} \| .*') AS articulo
FROM `ga360-250517.53461765.ga_sessions_*`,UNNEST(hits) AS hits
WHERE _TABLE_SUFFIX = '20200101' AND REGEXP_CONTAINS(hits.page.pagepath,r'[PpIi][MmRr]-[0-9]{7}')
)

SELECT
  date, region, city,
  campaign, source, medium, sku, articulo,
  --COUNT(fullVisitorId) AS usuarios,
  COUNT(DISTINCT fullVisitorId) AS usuariosUnicos,
  COUNT(sessionId) AS pageviews,
  COUNT(DISTINCT sessionId) AS uniquePageviews
FROM rawData
GROUP BY date, region, city,
  campaign, source, medium, sku, articulo
ORDER BY pageviews DESC