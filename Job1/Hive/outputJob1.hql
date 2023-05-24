DROP TABLE documents;
DROP TABLE info; 
DROP TABLE output_job1;
DROP TABLE topTen;
DROP TABLE topFive;
DROP TABLE mergedTable;

CREATE TABLE documents(Id INT, ProductId STRING, UserId STRING, ProfileName STRING, HelpfullnessNumerator INT, HelpfullnessDenominator INT, Score INT, time BIGINT, Summary STRING, text STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/fabio/ProgettoBigData/Reviews.csv' OVERWRITE INTO TABLE documents;

CREATE TABLE info AS
SELECT ProductId, YEAR(from_unixtime(CAST(time AS BIGINT))) AS time, text
FROM documents;


CREATE TABLE output_job1 AS
SELECT time, ProductId, val, parola, COUNT(*) AS conteggio_parola
FROM (
    SELECT time, ProductId, val, split(testo_concatenato, ' ') AS parole
    FROM (
        SELECT time, ProductId, COUNT(ProductId) AS val, concat_ws(' ', collect_list(text)) AS testo_concatenato
        FROM info
        GROUP BY time, ProductId
    ) t
) t2
LATERAL VIEW explode(parole) exploded_table AS parola
WHERE parola != '' AND LENGTH(parola)>=4
GROUP BY time, ProductId,val, parola;

CREATE TABLE topTen AS
SELECT time, productId, testoConcatenato, valoretesto_concatenato
FROM(
	SELECT *, row_number()
		over ( PARTITION BY time ORDER BY time, val DESC) as dimensione
	FROM (
		SELECT time, productId, val,concat_ws(' ', collect_list(parola)) as testoConcatenato, concat_ws(' ', collect_list(CAST(conteggio_parola AS STRING))) aS valoretesto_concatenato
		FROM output_job1
		GROUP BY time, productId, val
	) p
	ORDER BY time DESC, dimensione) dimensione_table
WHERE dimensione_table.dimensione < 11;

CREATE TABLE topFive AS
SELECT time, ProductId, parola, conteggio_parola
FROM (
    SELECT time, ProductId, parola, conteggio_parola,
           ROW_NUMBER() OVER (PARTITION BY time, ProductId ORDER BY conteggio_parola DESC) AS rn
    FROM output_job1
) t
WHERE rn <= 5;

CREATE TABLE mergedTable AS
SELECT tf.time, tf.productId, tf.parola, tf.conteggio_parola
FROM topFive tf
JOIN (
    SELECT time, productId
    FROM topTen
    GROUP BY time, productId
) tt ON tt.time = tf.time AND tt.productId = tf.productId;

SELECT * FROM mergedTable;

DROP TABLE documents;
DROP TABLE info; 
DROP TABLE output_job1;
DROP TABLE topTen;
DROP TABLE topFive;
DROP TABLE mergedTable;
