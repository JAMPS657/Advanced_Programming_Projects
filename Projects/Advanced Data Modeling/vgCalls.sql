/*This procedure should take 2 parameters: the minimal profit necessary to be in the returned 
results and the region*/
-- Calls to game profit by region
call gameProfitByRegion(35, 'WD');  /*should return every game title along with its profit 
                                       for the entire world (WD) where the profit exceeded 
									   30 million in sales.*/
call gameProfitByRegion(12, 'EU');
call gameProfitByRegion(10, 'JP');

/*This procedure should take 2 parameters: a genre name and region*/
-- Calls to genre ranking by region
call genreRankingByRegion('Sports', 'WD'); /*should return the profit ranking of all game 
                                              sales of ‘sports’ games across the entire world*/
call genreRankingByRegion('Role-playing', 'NA');
call genreRankingByRegion('Role-playing', 'JP');

/*This procedure should take 2 parameters: a publisher name and a genre name*/
-- Calls to published releases
call publishedReleases('Electronic Arts', 'Sports'); /*should return the total number of 
                                                        titles Electronic Arts has released 
														in the Sports genre*/
call publishedReleases('Electronic Arts', 'Action');

/*This procedure should take 4 parameters: game title, platform name, 
genre name, and publisher name*/
-- Calls to add new release
call addNewRelease('Foo Attacks', 'X360', 'Strategy', 'Stevenson Studios');
