/* codigo para numeros del proyect chapter */
-- select min( fec_fechaorden) , max(fec_fechaorden) from  `ga360-250517.cenic.compras_kiosco`   --  rango compras kiosk
SELECT TIMESTAMP_SECONDS( min( visitStartTime )), TIMESTAMP_SECONDS (max( visitStartTime ))   FROM `ga360-250517.147163492.ga_sessions_*` -- memoria de 13 meses de GA de todas las sesiones web
--SELECT TIMESTAMP_SECONDS( min( visitStartTime )), TIMESTAMP_SECONDS (max( visitStartTime ))   FROM `ga360-250517.53461765.ga_sessions_*` -- memoria de 13 meses de GA de todas las sesiones sessiones


