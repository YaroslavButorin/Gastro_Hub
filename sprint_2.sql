DROP TABLE IF EXISTS cafe.restaurants, 
           cafe.managers,
           cafe.sales,
           cafe.restaurant_manager_work_dates ;

-- Этап 1. Создание дополнительных таблиц
CREATE TYPE cafe.restaurant_type AS ENUM 
    ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

-- Создаем
CREATE TABLE cafe.restaurants (
    restaurant_uuid UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
    name text,
    location INTEGER,
    latitude double precision,
    longitude double precision,
    type cafe.restaurant_type,
    menu jsonb,
    FOREIGN KEY (location) REFERENCES cafe.districts(id)
);
-- Заполняем
INSERT INTO cafe.restaurants(name,location,type,menu,latitude,longitude)
SELECT DISTINCT(m.cafe_name),d.id,s.type::cafe.restaurant_type,m.menu,s.latitude,s.longitude
FROM raw_data.menu m
JOIN raw_data.sales s ON m.cafe_name = s.cafe_name
JOIN cafe.districts AS d ON ST_Within(
    ST_SetSRID(ST_MakePoint(s.longitude, s.latitude), 4326),
    d.district_geom
);
 
-- Создаем
CREATE TABLE cafe.managers (
    manager_uuid UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
    manager_name text,
    phone text
);
-- Заполняем
INSERT INTO cafe.managers(manager_name,phone)
SELECT DISTINCT(manager),manager_phone FROM raw_data.sales;

--Создаем
CREATE TABLE cafe.restaurant_manager_work_dates (
    restaurant_uuid UUID NOT NULL,
    manager_uuid UUID NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    FOREIGN KEY (restaurant_uuid) REFERENCES cafe.restaurants(restaurant_uuid),
    FOREIGN KEY (manager_uuid) REFERENCES cafe.managers(manager_uuid)
);
-- Заполняем
INSERT INTO cafe.restaurant_manager_work_dates(restaurant_uuid,manager_uuid,start_date,end_date)
SELECT
    r.restaurant_uuid AS restaurant_uuid,
    m.manager_uuid AS manager_uuid,
    MIN(s.report_date) AS min_report_date,
    MAX(s.report_date) AS max_report_date
FROM
    raw_data.sales s
JOIN
    cafe.restaurants r ON s.cafe_name = r.name
JOIN
    cafe.managers m ON s.manager = m.manager_name
GROUP BY
    r.restaurant_uuid,
    m.manager_uuid
ORDER BY
    r.restaurant_uuid,
    m.manager_uuid;


-- Создаем
CREATE TABLE cafe.sales (
    date DATE NOT NULL,
    restaurant_uuid UUID NOT NULL,
    avg_check NUMERIC(6,2),
    PRIMARY KEY (date,restaurant_uuid),
    FOREIGN KEY (restaurant_uuid) REFERENCES cafe.restaurants(restaurant_uuid)
);
-- Заполняем
INSERT INTO cafe.sales(date,restaurant_uuid,avg_check)
SELECT s.report_date,r.restaurant_uuid,s.avg_check
FROM raw_data.sales s
JOIN cafe.restaurants r ON s.cafe_name = r.name;
-- Этап 2. Создание представлений и написание аналитических запросов
--Задание 1
CREATE OR REPLACE VIEW top_restaurants_by_avg_check AS
    WITH avg_checks AS (
        SELECT
            r.name AS restaurant_name,
            r.type AS restaurant_type,
            ROUND(AVG(s.avg_check), 2) AS avg_check
        FROM
            cafe.sales s
        JOIN
            cafe.restaurants r ON s.restaurant_uuid = r.restaurant_uuid
        GROUP BY
            r.name,
            r.type
    ),
    ranked_restaurants AS (
        SELECT
            restaurant_name,
            restaurant_type,
            avg_check,
            RANK() OVER (PARTITION BY restaurant_type ORDER BY avg_check DESC) AS rank
        FROM
            avg_checks
    )
    SELECT
        restaurant_name,
        restaurant_type,
        avg_check
    FROM
        ranked_restaurants
    WHERE
        rank <= 3;
    --Задание 2
CREATE MATERIALIZED VIEW top_restaurants_by_avg_check AS
SELECT
	EXTRACT(YEAR FROM date) AS year,
	r.name AS restaurant_name,
	r.type AS restaurant_type,
	ROUND(AVG(s.avg_check), 2) AS avg_check,
	LAG(ROUND(AVG(s.avg_check), 2)) OVER (PARTITION BY r.name ORDER BY EXTRACT(YEAR FROM date)) AS previous_year_avg_check,
	ROUND((ROUND(AVG(s.avg_check), 2) - LAG(ROUND(AVG(s.avg_check), 2)) OVER (PARTITION BY r.name ORDER BY EXTRACT(YEAR FROM date))) / NULLIF(LAG(ROUND(AVG(s.avg_check), 2)) OVER (PARTITION BY r.name ORDER BY EXTRACT(YEAR FROM date)), 0) * 100, 2) AS change
FROM
	cafe.sales s
JOIN
	cafe.restaurants r ON s.restaurant_uuid = r.restaurant_uuid

	WHERE EXTRACT(YEAR FROM date) != 2023
GROUP BY
	EXTRACT(YEAR FROM date),
	r.name,
	r.type;
--Задание 3
SELECT r.name,
	COUNT(DISTINCT(manager_uuid)) AS manager_change_count
FROM cafe.restaurant_manager_work_dates work_dates
JOIN cafe.restaurants r ON work_dates.restaurant_uuid = r.restaurant_uuid
GROUP BY r.name
ORDER BY manager_change_count DESC
LIMIT 3;

--Задание 4
with top_pizza as (
	SELECT name,count(pizza) as pizza_count
		FROM (SELECT r.name,jsonb_object_keys(menu::jsonb ->'Пицца') AS pizza
				FROM cafe.restaurants r
				WHERE type = 'pizzeria'
		)
	GROUP BY name
	ORDER BY count(pizza) desc)

SELECT *
FROM top_pizza
WHERE pizza_count = (SELECT MAX(pizza_count) FROM top_pizza);
--Задание 5
SELECT 
	   DISTINCT ON (r.name)
	   r.name,
	   'Пицца' as type,
	   pizza_details.key as pizza_name,
	   MAX(pizza_details.value) as max_price
		
FROM cafe.restaurants r,
	 jsonb_each_text(r.menu::jsonb -> 'Пицца') AS pizza_details
WHERE type = 'pizzeria'
GROUP BY r.name,pizza_name
ORDER BY r.name, max_price DESC;
--Задание 6
WITH dist AS (
    SELECT 
        r1.name AS rest1,
        r1.type AS type,
        r2.name AS rest2,
        ST_Distance(
            ST_SetSRID(ST_Point(r1.longitude, r1.latitude), 4326)::geography,
            ST_SetSRID(ST_Point(r2.longitude, r2.latitude), 4326)::geography
        ) AS distance
    FROM 
        cafe.restaurants r1
    JOIN 
        cafe.restaurants r2 ON r1.type = r2.type
    WHERE 
        r1.name <> r2.name
)
SELECT 
    rest1,
    type,
    rest2,
    MIN(distance) AS min_distance
FROM 
    dist
GROUP BY 
    rest1, type, rest2
ORDER BY 
    min_distance ASC
LIMIT 1;
--Задание 7
WITH max_district AS (
    SELECT district_name, count(restaurant_uuid) AS restaurant_count
    FROM cafe.restaurants r
    JOIN cafe.districts d ON r.location = d.id
    GROUP BY district_name
    ORDER BY restaurant_count DESC
    LIMIT 1
), 
min_district AS (
    SELECT district_name, count(restaurant_uuid) AS restaurant_count
    FROM cafe.restaurants r
    JOIN cafe.districts d ON r.location = d.id
    GROUP BY district_name
    ORDER BY restaurant_count ASC
    LIMIT 1
)
SELECT * FROM max_district
UNION ALL
SELECT * FROM min_district;



