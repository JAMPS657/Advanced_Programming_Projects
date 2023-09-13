-- vgMigration Accomplishes 3 Things
-- 1.) Drop all tables that will be created in the following order
DROP TABLE IF EXISTS vg_releases;
DROP TABLE IF EXISTS vg_publishers;
DROP TABLE IF EXISTS vg_platforms;
DROP TABLE IF EXISTS vg_games;
DROP TABLE IF EXISTS vg_genres;

-- 2.) Use DDL create table commands to create the tables necessary. ALL table names 
--    begin with "vg_..."

-- Create Table vg_genres --
CREATE TABLE vg_genres(genre_id BIGINT AUTO_INCREMENT PRIMARY KEY,
					   genre VARCHAR(30) );
-- Create Table vg_games --
CREATE TABLE vg_games(game_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                      name VARCHAR(1000),
                      genre_id BIGINT,
CONSTRAINT vg_games_genre_id_fk FOREIGN KEY (genre_id) REFERENCES vg_genres(genre_id) );
-- Create Table vg_publishers --
CREATE TABLE vg_publishers(publisher_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                           publisher VARCHAR(50) );
-- Create Table vg_platforms --
CREATE TABLE vg_platforms(platform_id BIGINT AUTO_INCREMENT PRIMARY KEY,
						  platform VARCHAR(50) );
-- Create Table vg_releases --
CREATE TABLE vg_releases(release_id BIGINT AUTO_INCREMENT,
						 year DATE,
                         game_id BIGINT,
                         genre_id BIGINT,
                         platform_id BIGINT,
                         publisher_id BIGINT,
						 na_sales FLOAT,
                         eu_sales FLOAT,
                         jp_sales FLOAT,
                         other_sales FLOAT,
CONSTRAINT vg_games_game_id_fk FOREIGN KEY (game_id) REFERENCES vg_games(game_id),
CONSTRAINT vg_genres_genre_id_fk FOREIGN KEY (genre_id) REFERENCES vg_genres(genre_id),
CONSTRAINT vg_platforms_platform_id_fk FOREIGN KEY (platform_id) REFERENCES vg_platforms(platform_id),
CONSTRAINT vg_publishers_publisher_id_fk FOREIGN KEY (publisher_id) REFERENCES vg_publishers(publisher_id),
CONSTRAINT vg_releases_release_id_pk PRIMARY KEY (release_id, game_id, genre_id, platform_id, publisher_id) 
);

/*----------------Note: bad/missing data is handled during inserting--------*/
# 3. Data Migration you can use bulk inserts with nested queries to help with this task
-- Populate Table vg_genres
INSERT INTO vg_genres(genre_id, genre)
SELECT DISTINCT null, genre FROM vg_csv;
-- Populate Table vg_games 
INSERT INTO vg_games (game_id, name, genre_id)
SELECT null, name, genre_id
FROM vg_csv
JOIN vg_genres ON vg_genres.genre = vg_csv.genre;
-- Populate Table vg_publishers
INSERT INTO vg_publishers(publisher_id, publisher)
SELECT DISTINCT null, publisher FROM vg_csv;
-- Populate Table vg_platforms
INSERT INTO vg_platforms(platform_id, platform)
SELECT DISTINCT null, platform FROM vg_csv;
-- Populate Table vg_releases
-- Note that year in the vg_csv is still a string datatype, need to convert that to date
INSERT INTO vg_releases(release_id, year, game_id, genre_id, platform_id, publisher_id,
						 na_sales, eu_sales, jp_sales, other_sales) 
SELECT null, IF(year = 'N/A', '0000-00-00', STR_TO_DATE(year, '%Y')), 
       game_id, vg_genres.genre_id, platform_id, publisher_id,
	   na_sales, eu_sales, jp_sales, other_sales
FROM vg_csv
JOIN vg_games ON vg_games.name = vg_csv.name
JOIN vg_genres ON vg_genres.genre = vg_csv.genre
JOIN vg_platforms ON vg_platforms.platform = vg_csv.platform
JOIN vg_publishers ON vg_publishers.publisher = vg_csv.publisher;
