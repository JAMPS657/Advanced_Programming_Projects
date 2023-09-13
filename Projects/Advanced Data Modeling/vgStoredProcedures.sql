/*To make things easier for clients using your database, you will next create the following 
stored procedures that answer common queries/preform common tasks*/
-- --------------Drop any stored procedures made in this sql file-------------- 
DROP PROCEDURE IF EXISTS gameProfitByRegion;
DROP PROCEDURE IF EXISTS genreRankingByRegion;
DROP PROCEDURE IF EXISTS publishedReleases;
DROP PROCEDURE IF EXISTS addNewRelease;

-- --------------Creating 1st Stored Procedure gameProfitByRegion-------------- 
DELIMITER //
CREATE PROCEDURE gameProfitByRegion(min_profit INT, region CHAR(10))
BEGIN
    IF region = 'WD' THEN
        SELECT vg_games.name, (vg_releases.na_sales + vg_releases.eu_sales + vg_releases.jp_sales + vg_releases.other_sales) AS profit
        FROM vg_games
        JOIN vg_releases ON vg_games.game_id = vg_releases.game_id
        HAVING profit > min_profit;
    ELSEIF region = 'NA' THEN
        SELECT vg_games.title, vg_releases.na_sales AS profit
        FROM vg_games
        JOIN vg_releases ON vg_games.game_id = vg_releases.game_id
        HAVING profit > min_profit;
    ELSEIF region = 'EU' THEN
        SELECT vg_games.name, vg_releases.eu_sales AS profit
        FROM vg_games
        JOIN vg_releases ON vg_games.game_id = vg_releases.game_id
        HAVING profit > min_profit;
    ELSEIF region = 'JP' THEN
        SELECT vg_games.name, vg_releases.jp_sales AS profit
        FROM vg_games
        JOIN vg_releases ON vg_games.game_id = vg_releases.game_id
        HAVING profit > min_profit;
    END IF;
END//
DELIMITER ;

-- --------------Creating 2nd Stored Procedure genreRankingByRegion INCOMPLETE--------------
DELIMITER //
CREATE PROCEDURE genreRankingByRegion(
IN genreName VARCHAR(30),
IN region CHAR(2)
)
BEGIN
DECLARE regionValid BOOLEAN DEFAULT FALSE;
DECLARE regionList VARCHAR(100);
-- Set region list based on the input region parameter
CASE region 
    WHEN 'NA' THEN SET regionList = 'NA_sales';
    WHEN 'EU' THEN SET regionList = 'EU_sales';
    WHEN 'JP' THEN SET regionList = 'JP_sales';
    WHEN 'Other' THEN SET regionList = 'Other_sales';
    WHEN 'WD' THEN SET regionList = 'NA_sales + EU_sales + JP_sales + Other_sales';
    ELSE SET regionList = NULL;
END CASE;
-- Check if the input region is valid
IF regionList IS NOT NULL THEN
    SET regionValid = TRUE;
END IF;
IF NOT regionValid THEN
    SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'Invalid region parameter. Valid regions are: NA, EU, JP, Other, WD.';
ELSE
    -- Build the query to get the profit ranking
    SET @query = CONCAT('SELECT genre, SUM(', regionList, ') AS profit
                        FROM vg_releases
                        JOIN vg_genres ON vg_genres.genre_id = vg_releases.genre_id
                        WHERE genre = ''', genreName, '''
                        GROUP BY genre
                        ORDER BY profit DESC;');
    -- Execute the query
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END IF;
END//
DELIMITER ;

-- --------------Creating 3rd Stored Procedure publishedReleases--------------
DELIMITER //
CREATE PROCEDURE publishedReleases(IN publisherName VARCHAR(255), IN genreName VARCHAR(255))
BEGIN
    SELECT publisher,COUNT(*) AS num_releases
    FROM vg_releases
    JOIN vg_games ON vg_games.game_id = vg_releases.game_id
    JOIN vg_genres ON vg_genres.genre_id = vg_games.genre_id
    JOIN vg_publishers ON vg_publishers.publisher_id = vg_releases.publisher_id
    WHERE vg_genres.genre = genreName
    AND vg_publishers.publisher = publisherName;
END //
DELIMITER ;

-- --------------Creating 4th Stored Procedure addNewRelease--------------
DELIMITER //
CREATE PROCEDURE addNewRelease(
IN gameTitle VARCHAR(1000),
IN platformName VARCHAR(50),
IN genreName VARCHAR(30),
IN publisherName VARCHAR(50)
)
BEGIN
DECLARE platformId BIGINT;
DECLARE genreId BIGINT;
DECLARE publisherId BIGINT;
-- Check if the platform already exists in vg_platforms
SELECT platform_id INTO platformId FROM vg_platforms WHERE platform = platformName;
IF platformId IS NULL THEN
    -- Add new platform to vg_platforms
    INSERT INTO vg_platforms (platform) VALUES (platformName);
    SET platformId = LAST_INSERT_ID();
END IF;
-- Check if the genre already exists in vg_genres
SELECT genre_id INTO genreId FROM vg_genres WHERE genre = genreName;
IF genreId IS NULL THEN
    -- Add new genre to vg_genres
    INSERT INTO vg_genres (genre) VALUES (genreName);
    SET genreId = LAST_INSERT_ID();
END IF;
-- Check if the publisher already exists in vg_publishers
SELECT publisher_id INTO publisherId FROM vg_publishers WHERE publisher = publisherName;
IF publisherId IS NULL THEN
    -- Add new publisher to vg_publishers
    INSERT INTO vg_publishers (publisher) VALUES (publisherName);
    SET publisherId = LAST_INSERT_ID();
END IF;
-- Add new release to vg_releases
INSERT INTO vg_releases (year, game_id, genre_id, platform_id, publisher_id)
SELECT NULL, g.game_id, genreId, platformId, publisherId
FROM vg_games g
WHERE g.name = gameTitle;

SELECT CONCAT('New release added: ', gameTitle, ' on platform ', platformName, 
    ' in genre ', genreName, ' by publisher ', publisherName) AS message;
END //
DELIMITER ;