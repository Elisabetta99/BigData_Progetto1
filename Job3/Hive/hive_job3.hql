DROP TABLE IF EXISTS reviews_job3;

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

-- Creazione della tabella filtered_reviews (filtro prodotti con score >= 4)
CREATE TABLE IF NOT EXISTS filtered_reviews AS
SELECT UserId, ProductId, Score
FROM reviews_job3
WHERE Score >= 4;

-- Creazione della tabella user_groups per gli utenti con 3 prodotti in comune
CREATE TABLE IF NOT EXISTS user_groups AS
SELECT a.UserId AS user1, collect_set(b.UserId) AS users, collect_set(b.ProductId) AS common_products
FROM (
    SELECT DISTINCT UserId, ProductId
    FROM filtered_reviews
) a
JOIN (
    SELECT DISTINCT UserId, ProductId
    FROM filtered_reviews
) b ON a.ProductId = b.ProductId AND a.UserId < b.UserId
GROUP BY a.UserId
HAVING size(collect_set(b.ProductId)) >= 3;

-- Creazione della tabella affinity_groups con un unico gruppo contenente tutti gli utenti con gli stessi prodotti in comune
CREATE TABLE IF NOT EXISTS affinity_groups AS
SELECT collect_set(user1) AS users, common_products
FROM user_groups

GROUP BY common_products;

-- Ordinamento dei gruppi in base all'UserId del primo utente del gruppo
CREATE TABLE IF NOT EXISTS sorted_groups AS
SELECT users, common_products
FROM affinity_groups
WHERE size(users) > 1
ORDER BY users[0];


-- Scrittura del risultato nella cartella di output
INSERT OVERWRITE DIRECTORY 'file:///home/elisabetta/Scrivania/output_hive3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM sorted_groups;

