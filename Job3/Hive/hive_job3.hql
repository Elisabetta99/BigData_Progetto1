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

-- Creazione di gruppi di utenti con gusti affini
CREATE TABLE user_groups AS
SELECT concat_ws(',', collect_list(DISTINCT UserId)) AS GroupUsers, ProductId, COUNT(*) AS CommonProducts
FROM reviews_job3
WHERE Score >= 4
GROUP BY ProductId
HAVING CommonProducts >= 3;

-- Selezione dei gruppi con almeno 3 utenti e ordinamento per UserId del primo elemento
CREATE TABLE similar_user_groups AS
SELECT GroupUsers, collect_set(ProductId) AS SharedProducts
FROM user_groups
WHERE size(split(GroupUsers, ',')) >= 2
GROUP BY GroupUsers
ORDER BY split(GroupUsers, ',')[0];


INSERT OVERWRITE DIRECTORY 'home/elisabetta/Scrivania/BigData'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM similar_user_groups;

