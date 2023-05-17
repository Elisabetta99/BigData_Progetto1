DROP TABLE reviews;
DROP TABLE reviews_job3;

SET hive.strict.checks.cartesian.product=false;

CREATE TABLE IF NOT EXISTS reviews_job3 (
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

LOAD DATA LOCAL INPATH '/home/elisabetta/Scrivania/Reviews.csv' OVERWRITE INTO TABLE reviews_job3;

CREATE TABLE filtered_products AS
SELECT *
FROM reviews_job3
WHERE Score >= 4;

CREATE TABLE filtered_users AS
SELECT UserId
FROM (
    SELECT UserId, COUNT(*) AS product_count
    FROM (
        SELECT DISTINCT UserId, ProductId
        FROM filtered_products
    ) p
    GROUP BY UserId
) u
WHERE product_count >= 3;

-- Crea la tabella user_groups con gli utenti che condividono almeno 3 prodotti in comune
CREATE TABLE IF NOT EXISTS user_groups AS
SELECT a.UserId AS user1, b.UserId AS user2
FROM (
    SELECT DISTINCT UserId, ProductId
    FROM filtered_products
) a
JOIN (
    SELECT DISTINCT UserId, ProductId
    FROM filtered_products
) b ON a.ProductId = b.ProductId AND a.UserId < b.UserId
GROUP BY a.UserId, b.UserId
HAVING COUNT(*) >= 3;

CREATE TABLE user_group_products AS
SELECT g.user1, g.user2, collect_set(p1.ProductId) AS shared_products
FROM user_groups g
JOIN filtered_products p1 ON g.user1 = p1.UserId
JOIN filtered_products p2 ON g.user2 = p2.UserId
WHERE p1.ProductId = p2.ProductId
GROUP BY g.user1, g.user2;


CREATE TABLE sorted_groups AS
SELECT user1, user2, shared_products
FROM user_group_products
ORDER BY user1;


INSERT OVERWRITE DIRECTORY 'home/elisabetta/Scrivania/BigData'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM sorted_groups;

