/* Селектим все необходимые поля из обоих подзапросов, т.к. нужно иерархическое заполнение с верхних уровней для корректного заполнения применяем аггрегирующую функцию STRING_AGG*/
SELECT dat.segment, dat.domain, rec.id, rec.app_name, max(dat.func_id) AS func_id, max(dat.func_name) AS func_name, 
       string_agg(dat.status,'/ ') AS status, string_agg(dat.rsm_app_name,'/ ') AS rsm_app_name, string_agg(dat.asset,'/ ') AS asset
FROM /*Рекурсивный подзапрос и JOIN с иерархической таблицей в БД*/
    (
    WITH RECURSIVE parnt as
        (
        SELECT id, parent_app_id, name AS app_name
        FROM fx_app
        UNION ALL 
        SELECT fa.id, fa.parent_app_id, fa.name
        FROM fx_app fa 
        JOIN parnt ON fa.parent_app_id = parnt.id
        )
    SELECT id, parent_app_id, app_name FROM parnt
    UNION ALL
    SELECT id, id, name FROM fx_app
    ) rec,
    ( /*Первая часть подзапроса собирает все applications*/
    SELECT ts.*, fai.status, ea.name AS rsm_app_name, ea.asset
    FROM 
        (
        SELECT distinct(upper(segment)) AS segment, domain_id AS domain, fa.parent_app_id, fa.id, fa.name AS app_name,  null AS func_id, null AS func_name
        FROM fx_app_implementation fai, 
             fx_app fa
        ) ts
    JOIN fx_app_implementation fai ON fai.fx_app_id = ts.id AND ts.segment = upper(fai.segment)
    JOIN er_app ea ON ea.id = fai.app_id 
    UNION /*Вторая часть подзапроса собирает все functions*/
    SELECT fas.*, eaf.status, ea.name AS rsm_app_name, ea.asset
    FROM 
        (
        SELECT DISTINCT (upper(segment)) AS segment, domain_id AS domain,fa.parent_app_id, fa.id AS app_id, fa.name AS app_name, ff.id, ff.name AS func_name
        FROM fx_app fa,
             fx_function ff,
             fx_app_function faf,
             fx_app_implementation fai
        WHERE fa.id = faf.app_id
          AND ff.id = faf.function_id 
        ) fas
    LEFT JOIN er_app_function eaf ON eaf.function_id = fAS.id AND fAS.segment = upper(eaf.segment) 
    LEFT JOIN er_app ea ON ea.id = eaf.app_id
    ) dat 
WHERE dat.id = rec.parent_app_id /*Связываем оба подзапроса*/
GROUP BY dat.segment, dat.domain, rec.id, rec.app_name 

