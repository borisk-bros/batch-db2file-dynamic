DROP TABLE people IF EXISTS;

CREATE TABLE people  (
    person_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    first_name VARCHAR(20),
    last_name VARCHAR(20)
);

INSERT INTO people VALUES (1, 'Sumeet', 'Sharma');
INSERT INTO people VALUES (2, 'Matthew', 'Gupta');
INSERT INTO people VALUES (3, 'Joseph', 'Lee');
INSERT INTO people VALUES (4, 'Jim', 'Wolsey');
INSERT INTO people VALUES (5, 'Sai', 'Sibbala');
INSERT INTO people VALUES (6, 'Gigi', 'Bhullar');


