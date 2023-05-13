-- Creazione tabella
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

-- Caricamento dati
LOAD DATA LOCAL INPATH '/home/elisabetta/Scrivania/BigData/Reviews.csv' overwrite INTO TABLE user_reviews;

-- Calcolo apprezzamento per ogni utente
CREATE TABLE user_appreciation AS
SELECT
    UserId,
    SUM(CASE WHEN HelpfulnessDenominator != 0 THEN HelpfulnessNumerator / HelpfulnessDenominator ELSE 0 END) / COUNT(*) AS appreciation
FROM user_reviews
GROUP BY UserId;

-- Ordinamento della lista di utenti in base all'apprezzamento
CREATE TABLE sorted_users AS
SELECT UserId, appreciation
FROM user_appreciation
ORDER BY appreciation DESC;

-- Visualizzazione dei risultati
SELECT *
FROM sorted_users;