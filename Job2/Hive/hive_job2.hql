CREATE TABLE IF NOT EXIST user_reviews (
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
COMMENT 'User Reviews Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/elisabetta/Scrivania/BigData/Reviews.csv' overwrite INTO TABLE user_reviews;

CREATE TABLE user_appreciation AS
SELECT
    UserId,
    AVG(HelpfulnessNumerator / HelpfulnessDenominator) AS appreciation
FROM user_reviews
GROUP BY UserId;

CREATE TABLE sorted_users AS
SELECT UserId, appreciation
FROM user_appreciation
ORDER BY appreciation DESC;