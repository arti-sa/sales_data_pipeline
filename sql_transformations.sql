CREATE TABLE IF NOT EXISTS staging.sales_raw (
    salesperson_id      INT,
    salesperson_name    VARCHAR(100),
    sales_state_id      INT,
    sales_state_name    VARCHAR(100),
    store_id            INT,
    store_name          VARCHAR(100),
    year                INT,
    Jan_sales           DECIMAL(18,2),
    Feb_sales           DECIMAL(18,2),
    Mar_sales           DECIMAL(18,2),
    Apr_sales           DECIMAL(18,2),
    May_sales           DECIMAL(18,2),
    Jun_sales           DECIMAL(18,2),
    Jul_sales           DECIMAL(18,2),
    Aug_sales           DECIMAL(18,2),
    Sep_sales           DECIMAL(18,2),
    Oct_sales           DECIMAL(18,2),
    Nov_sales           DECIMAL(18,2),
    Dec_sales           DECIMAL(18,2),
    source_file         VARCHAR(255),
    load_timestamp      TIMESTAMP
);


CREATE TABLE IF NOT EXISTS curated.sales_normalized (
    record_id           BIGINT IDENTITY(1,1),
    source_file         VARCHAR(255),
    load_timestamp      TIMESTAMP,
    salesperson_id      INT NOT NULL,
    salesperson_name    VARCHAR(100),
    sales_state_id      INT,
    sales_state_name    VARCHAR(100),
    store_id            INT,
    store_name          VARCHAR(100),
    year                INT NOT NULL,
    month               VARCHAR(3) NOT NULL,
    month_number        INT NOT NULL,
    sales_amount        DECIMAL(18,2),
    quarter             INT,

    CONSTRAINT pk_sales_normalized PRIMARY KEY (record_id)
);


INSERT INTO curated.sales_normalized (
    source_file,
    load_timestamp,
    salesperson_id,
    salesperson_name,
    sales_state_id,
    sales_state_name,
    store_id,
    store_name,
    year,
    month,
    month_number,
    sales_amount,
    quarter
)
SELECT
    source_file,
    load_timestamp,
    salesperson_id,
    salesperson_name,
    sales_state_id,
    sales_state_name,
    store_id,
    store_name,
    year,
    month,
    month_number,
    sales_amount,
    CEILING(month_number / 3.0) AS quarter
FROM staging.sales_raw
UNPIVOT (
    sales_amount FOR month IN (
        Jan_sales AS 'Jan',
        Feb_sales AS 'Feb',
        Mar_sales AS 'Mar',
        Apr_sales AS 'Apr',
        May_sales AS 'May',
        Jun_sales AS 'Jun',
        Jul_sales AS 'Jul',
        Aug_sales AS 'Aug',
        Sep_sales AS 'Sep',
        Oct_sales AS 'Oct',
        Nov_sales AS 'Nov',
        Dec_sales AS 'Dec'
    )
) AS unpivoted
CROSS JOIN (
    SELECT
        CASE month
            WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3
            WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6
            WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9
            WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
        END AS month_number
) AS months
WHERE sales_amount IS NOT NULL;


INSERT INTO curated.sales_normalized (
    source_file, load_timestamp, salesperson_id, salesperson_name,
    sales_state_id, sales_state_name, store_id, store_name,
    year, month, month_number, sales_amount, quarter
)
SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Jan', 1, Jan_sales, 1
FROM staging.sales_raw WHERE Jan_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Feb', 2, Feb_sales, 1
FROM staging.sales_raw WHERE Feb_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Mar', 3, Mar_sales, 1
FROM staging.sales_raw WHERE Mar_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Apr', 4, Apr_sales, 2
FROM staging.sales_raw WHERE Apr_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'May', 5, May_sales, 2
FROM staging.sales_raw WHERE May_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Jun', 6, Jun_sales, 2
FROM staging.sales_raw WHERE Jun_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Jul', 7, Jul_sales, 3
FROM staging.sales_raw WHERE Jul_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Aug', 8, Aug_sales, 3
FROM staging.sales_raw WHERE Aug_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Sep', 9, Sep_sales, 3
FROM staging.sales_raw WHERE Sep_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Oct', 10, Oct_sales, 4
FROM staging.sales_raw WHERE Oct_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Nov', 11, Nov_sales, 4
FROM staging.sales_raw WHERE Nov_sales IS NOT NULL

UNION ALL

SELECT source_file, load_timestamp, salesperson_id, salesperson_name,
       sales_state_id, sales_state_name, store_id, store_name,
       year, 'Dec', 12, Dec_sales, 4
FROM staging.sales_raw WHERE Dec_sales IS NOT NULL;


CREATE VIEW analytics.sales_by_quarter AS
SELECT
    year,
    quarter,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT salesperson_id) AS active_salespeople,
    COUNT(DISTINCT store_id) AS active_stores
FROM curated.sales_normalized
GROUP BY year, quarter;


CREATE VIEW analytics.sales_by_salesperson AS
SELECT
    salesperson_id,
    salesperson_name,
    year,
    SUM(sales_amount) AS total_sales,
    AVG(sales_amount) AS avg_monthly_sales,
    MAX(sales_amount) AS max_monthly_sales,
    MIN(sales_amount) AS min_monthly_sales
FROM curated.sales_normalized
GROUP BY salesperson_id, salesperson_name, year;


CREATE VIEW analytics.sales_by_state AS
SELECT
    sales_state_id,
    sales_state_name,
    year,
    month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT store_id) AS store_count
FROM curated.sales_normalized
GROUP BY sales_state_id, sales_state_name, year, month;


SELECT 'Null salesperson_id' AS issue, COUNT(*) AS count
FROM staging.sales_raw WHERE salesperson_id IS NULL
UNION ALL
SELECT 'Null store_id', COUNT(*)
FROM staging.sales_raw WHERE store_id IS NULL
UNION ALL
SELECT 'Null year', COUNT(*)
FROM staging.sales_raw WHERE year IS NULL;


SELECT month, COUNT(*) AS negative_count
FROM curated.sales_normalized
WHERE sales_amount < 0
GROUP BY month;


SELECT salesperson_id, store_id, year, month, COUNT(*) AS duplicate_count
FROM curated.sales_normalized
GROUP BY salesperson_id, store_id, year, month
HAVING COUNT(*) > 1;