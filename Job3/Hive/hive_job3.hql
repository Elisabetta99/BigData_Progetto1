DROP TABLE reviews;

-- Crea la tabella delle recensioni
CREATE TABLE IF NOT EXISTS reviews (
    Id int,
    ProductId string,
    UserId string,
    ProfileName string,
    HelpfulnessNumerator int,
    HelpfulnessDenominator int,
    Score int,
    Time int,
    Summary string,
    Text string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Carica i dati nella tabella delle recensioni
LOAD DATA LOCAL INPATH '/home/elisabetta/Scrivania/BigData/Reviews.csv' OVERWRITE INTO TABLE reviews;

-- Crea la tabella dei prodotti con score >= 4
CREATE TABLE high_score_products AS
SELECT DISTINCT ProductId
FROM reviews
WHERE Score >= 4;

-- Crea la tabella degli utenti che hanno recensito almeno 3 prodotti con score >= 4
CREATE TABLE users_with_common_products AS
SELECT UserId, COUNT(DISTINCT ProductId) AS common_product_count
FROM reviews
WHERE Score >= 4
GROUP BY UserId
HAVING COUNT(DISTINCT ProductId) >= 3;

-- Crea la tabella dei gruppi di utenti con gusti affini
CREATE TABLE user_groups AS
SELECT a.UserId AS GroupUserId, b.UserId, a.ProductId AS CommonProduct
FROM reviews a
JOIN reviews b ON a.ProductId = b.ProductId
JOIN users_with_common_products u ON a.UserId = u.UserId AND b.UserId != a.UserId
WHERE a.Score >= 4
  AND b.Score >= 4;

-- Ottieni gruppi di utenti con prodotti in comune
WITH RECURSIVE grouped_users AS (
  SELECT GroupUserId, UserId, CommonProduct, CAST(CONCAT(',', GroupUserId, ',', UserId, ',') AS STRING) AS GroupPath
  FROM user_groups
  WHERE GroupUserId IN (SELECT UserId FROM users_with_common_products)
  
  UNION ALL
  
  SELECT g.GroupUserId, u.UserId, u.CommonProduct, CAST(CONCAT(g.GroupPath, u.UserId, ',') AS STRING)
  FROM grouped_users g
  JOIN user_groups u ON g.CommonProduct = u.CommonProduct
    AND g.GroupPath NOT LIKE CONCAT('%,', u.UserId, ',%')
    AND u.UserId NOT IN (SELECT UserId FROM users_with_common_products)
)
SELECT DISTINCT GroupUserId, GroupPath, COLLECT_SET(CommonProduct) AS CommonProducts
FROM grouped_users
GROUP BY GroupUserId, GroupPath;

-- Ordina e visualizza i risultati
SELECT GroupUserId, CommonProducts
FROM (
  SELECT GroupUserId, CommonProducts, ROW_NUMBER() OVER (ORDER BY GroupUserId) AS row_num
  FROM (
    SELECT GroupUserId, CommonProducts
    FROM user_groupss_result
    LATERAL VIEW EXPLODE(CommonProducts) exploded_table AS CommonProduct
    GROUP BY GroupUserId, CommonProducts
  ) t1
) t2
WHERE row_num = 1;


INSERT OVERWRITE DIRECTORY '/home/elisabetta/PycharmProjects/BigData_Progetto1/Job3/Hive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','

SELECT *
FROM GroupUserId, CommonProducts;