-- Rode no Databricks SQL (ajuste catalog.schema abaixo ao seu ambiente).

-- 1) Campos cujo título menciona rastreio / transportadoras
SELECT id, title, type
FROM seu_catalogo.seu_schema.ticket_fields
WHERE LOWER(title) LIKE '%rastre%'
   OR LOWER(title) LIKE '%rastreamento%'
   OR LOWER(title) LIKE '%correios%'
   OR LOWER(title) LIKE '%jadlog%'
   OR LOWER(title) LIKE '%loggi%'
   OR LOWER(title) LIKE '%quantidade%'
ORDER BY title;

-- 2) Amostra de valores recentes para um field_id específico (substitua o ID):
-- SELECT
--   get_json_object(cf_item, '$.value') AS val,
--   COUNT(*) AS n
-- FROM seu_catalogo.seu_schema.tickets t
-- LATERAL VIEW EXPLODE(FROM_JSON(t.custom_fields, 'ARRAY<STRING>')) e AS cf_item
-- WHERE get_json_object(cf_item, '$.id') = 'SEU_FIELD_ID_AQUI'
--   AND CAST(t.updated_at AS DATE) >= CURRENT_DATE - INTERVAL 30 DAYS
-- GROUP BY 1
-- ORDER BY n DESC
-- LIMIT 30;
