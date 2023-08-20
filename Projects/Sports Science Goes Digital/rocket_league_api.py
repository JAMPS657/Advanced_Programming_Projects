'''
Author: Andrew J. Otis
'''
from IPython.display import Image, display
from IPython.display import YouTubeVideo

''' The following function will allow for .jpg images to be into the notebook'''
from IPython.display import Image, display
def place_image(pic_name):
    image_path = pic_name
    image = Image(filename=image_path)
    display(image)

place_image("front_cover.jpg")
place_image("playing_field.jpg")


#%% Necessary Packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ast
import requests
from bs4 import BeautifulSoup
# Run the following install code in order to access the module ballchasing, 
#  if necessary.
            # pip install python-ballchasing <-- run in terminal
import ballchasing


# Change working directory to utilize relative file paths
import os

# Change the working directory to a new directory
os.chdir('C:/Users/andio/OneDrive/Desktop/PythonProjectsDU/COMP4448_DS_Tools_II/Final_Project/')

# Get the current working directory
cwd = os.getcwd()
print("Current working directory:", cwd)

#%% -----------The Goal of the Project-----------
'''
1. To scrape available data online
2. To clean scraped data
3. Perform analysis on the data
4. Discuss implications or alternative uses
'''

#%% -----------Initialize API w/ token-----------
'''Documentation
https://ballchasing.com # Main page
https://ballchasing.com/doc/api # API documentation
https://ballchasing.com/doc/faq # General info
'''
# Perform the following in order to access the module ballchasing
# pip install python-ballchasing <-- run in terminal
import ballchasing

api = ballchasing.Api("4cuopOUxYPSc9LFwmlFFSDfLysN18HmRwf2JYb8o")


#%% -----------Pulling Specific Replay via hyperlink section-----------
replay = api.get_replay("52a56379-b02b-4344-90da-9d1dec37a1e8")
print(replay)

# flatten the list of dictionaries with pd.json_normalize()
single_replay_data = pd.json_normalize(replay) 
                                          
single_replay_data

#%% -----------Pulling Multiple Replays (All hrefs)-----------
import requests
from bs4 import BeautifulSoup
'''
The following function returns all HTML info on all the hyperlinks to replays
present on the homepage
'''
def get_href():
    # define the starting URL
    start_url = "https://ballchasing.com/"
    # send an HTTP GET request to the starting URL
    response = requests.get(start_url)
    # parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    # find all links in the HTML content
    hyperlinks = soup.find_all("a")
    
    # loop through the hyperlinks and perform any desired actions
    for hyperlink in hyperlinks:
        link_url = hyperlink.get("href")
        print(link_url)
        
        return hyperlinks

hyperlinks = get_href()
hyperlinks

#%% -----------Pulling Multiple Replays (replay data href)-----------
# Adjust the function get_href to only include href that gives replay 
#  data and name it get_replay_links
'''
The following function returns a dataframe of links for api.get_replay() to 
call upon for data analysis.
'''
def get_replay_links(start_url):
    # send an HTTP GET request to the starting URL
    response = requests.get(start_url)
    # parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    # find all links in the HTML content
    replays = soup.find_all("a")

    replay_links = []
    for hyperlink in replays:
        link_url = hyperlink.get("href")
        if link_url and link_url.startswith("/replay"):
            replay_links.append({"link": link_url})
    replays = pd.DataFrame(replay_links)
    replays['link'] = replays['link'].str.replace('/replay/', '')
    
    return replays

'''
The following function utilizes the replay links gathered to call the replay
data
'''
def call_and_compile(start_link):
    # run previous function to get multiple replay hrefs
    replay_links = get_replay_links(start_link)

    # A list of the links to loop through with API
    replay_links_list = replay_links['link'].tolist()

    # create an empty list to hold the resulting dataframes
    replays_dfs = []
    # loop through the links in replay_links_list 
    for link in replay_links_list:
        try:
            # get the replay data for the current link
            replay = api.get_replay(link)
            # json_normalize() function can handle nested JSON data and can 
            #  produce a flattened DataFrame.
            replay_df = pd.json_normalize(replay)
            # append the dataframe to the list of dataframes
            replays_dfs.append(replay_df)
        # the following block of code accounts for links that refused to be
        # scraped for whatever reason, by skipping it and contiuing the loop
        # to completion
        except Exception as e:
            print(f"Error occurred with replay link {link}: {e}")
            continue
    
    combined_replay_data = pd.concat(replays_dfs, ignore_index=True)
    
    return combined_replay_data

# Scrape the data one page at a time
replays_data = call_and_compile("https://ballchasing.com/?after=MS41MjIxMTMwMDY0OThlKzEyX2I1NmMxNjBlLTNjZTctNDVkZS05YzhjLTg0YTQ2YTE1MmJlOQ%3D%3D&playlist=12&playlist=13&playlist=10&playlist=11&sort-by=created&sort-dir=asc")

replays_data2 = call_and_compile("https://ballchasing.com/?after=MS41MjU5NzMxODc1MjFlKzEyXzE0NzdhYmNhLTdmOTUtNGQyYi04MmRiLWU0MzE3MzBiYmY2Zg%3D%3D&playlist=12&playlist=13&playlist=10&playlist=11&sort-by=created&sort-dir=asc")

#more_replays_data = call_and_compile("https://ballchasing.com/?after=MS42Nzg0MDMwMjU4NDFlKzEyXzhhNmE4MjZmLTM5NmEtNGRmZC05MDI0LWFhYjQ4YTZkOWJjOQ%3D%3D")

#%% -----------Create Copies of Scraped Data-----------
# Make a copy of the dataframes to avoid the need to re-scrape everytime
#  the dataframe is altered.
df = replays_data.copy(deep = True)
df.info()

df2 = replays_data2.copy(deep = True)
df2.info()

# Columns in df that are not in df2
diff1 = set(df.columns) - set(df2.columns)
print(diff1)

# Columns in df2 that are not in df
diff2 = set(df2.columns) - set(df.columns)
print(diff2)

#%% -----------Dealing with NaN values-----------
'''
The following function checks a datframe to return columns that contain at 
least one nan value and the count of nan's present in their corresponding
column
'''
def nan_vals(df):
    # count the NaN values in each column of the dataframe
    nan_count = df.isna().sum()
    # create a variable (i.e. mask) for selecting columns with nan_count 
    #  greater than 0
    mask = nan_count > 0
    # use the mask to select only the relevant columns
    cols_with_nans = df.loc[:, mask]
    cols_with_nans = cols_with_nans.isna().sum()
    return cols_with_nans

'''
The following function fills nan values with zeroes of specificed columns
'''
def fill_withzero(df):
    # Don't just drop NA values willy-nilly, make sure the data is understood.
    #  For example, the column overtime_seconds isn't out of place by having 
    #  them. Not every game will go to overtime. What makes sense here would 
    #  be to convert all the nan values in the column to 0
    if 'overtime_seconds' in df:
        df['overtime_seconds'] = df['overtime_seconds'].fillna(0)
    
    # Taking a look at the team stats, we see that each color has some nan's. 
    #  What this likely is teams that did not perform those in-game actions.
    #  For example, if the data was of soccer games. The rows would 
    #  likely have nan for goalies in the column "shots_on_goal". So just set 
    #  these nan values to zero
    if 'blue.stats.ball.possession_time' in df:
        df['blue.stats.ball.possession_time'] = df['blue.stats.ball.possession_time'].fillna(0)
    if 'blue.stats.ball.time_in_side' in df:
        df['blue.stats.ball.time_in_side'] = df['blue.stats.ball.time_in_side'].fillna(0)
    if 'orange.stats.ball.possession_time' in df:
        df['orange.stats.ball.possession_time'] = df['orange.stats.ball.possession_time'].fillna(0)
    if 'orange.stats.ball.time_in_side' in df:
        df['orange.stats.ball.time_in_side'] = df['orange.stats.ball.time_in_side'].fillna(0)

    nan_cols = nan_vals(df)
    
    return nan_cols

nan_cols = nan_vals(df)

nan_cols = fill_withzero(df)
nan_cols

# --------For df2
nan_cols_df2 = nan_vals(df2)

nan_cols_df2 = fill_withzero(df2)
nan_cols_df2

#%% -----------Droping columns-----------
'''
The following function drops columns that do not aid in data analysis and drops
any remaining rows that contain nan
'''
def drop_nan_rows(df):
    # drop ALL rows (i.e., replay metrics of a game) with null values
    df.dropna(axis=0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    return df

df = drop_nan_rows(df)
nan_vals(df) 


# --------For df2
df2 = drop_nan_rows(df2)

nan_vals(df2) # dataframe should be empty of nan's at this point

#%% Combine your web scrape dfs to combine samples

# Reset index so that there aren't repeating index values
replays_df = pd.concat([df, df2]).reset_index(drop=True)

#%% -----------Exploratory Data Analysis-----------
'''Feature Description of Data
'''
replays_df.info()

# Save as csv and Load in the dataframe so you dont have to re-scrape 

#replays_df.to_csv('replays_df.csv' ,index=True)
#replays_df = pd.read_csv('C:/Users/andio/OneDrive/Desktop/PythonProjectsDU/COMP4447_DS_Tools_I/Potential_Projects/Rocket_League_replay/replays_df_no_index.csv')
#replays_df.head(5)

#%% -----------In-game team statistics
# isolate in-game match data
team_stats = replays_df.iloc[:, 13:]

# Split the team's data into their own dataframe
blue_team_stats = team_stats.drop(team_stats.columns[52:], axis=1)
blue_team_stats = blue_team_stats.drop('blue.players', axis=1)
blue_team_stats.columns = blue_team_stats.columns.str.replace('blue.', '')

orange_team_stats = team_stats.iloc[:, 52:]
orange_team_stats = orange_team_stats.drop('orange.players', axis=1)
orange_team_stats.columns = orange_team_stats.columns.str.replace('orange.', '')

# Take care of the extra column from orange_team_stats after the slice
if 'overtime_seconds' in orange_team_stats:
    orange_team_stats = orange_team_stats.drop('overtime_seconds', axis=1)

if 'min_rank.name' in orange_team_stats:
    orange_team_stats = orange_team_stats.drop('min_rank.name', axis=1)


#%% -----------Making Tables of Each Game for Team Stats

blue = []
for i in range(len(blue_team_stats)):
    blue_transposed = blue_team_stats.iloc[i:i+1,:11].transpose()
    print(blue_team_stats.iloc[i:i+1,:11].transpose())
    blue.append(blue_transposed)
    print('\n')

orange = []
for i in range(len(orange_team_stats)):
    orange_transposed = orange_team_stats.iloc[i:i+1,:11].transpose()
    print(orange_team_stats.iloc[i:i+1,:11].transpose())
    orange.append(orange_transposed)
    print('\n')

# Combine the lists "blue" and "orange", to get a list of tabulated
# representation for each game in the data set.

combined_list = []
for blue_transposed, orange_transposed in zip(blue, orange):
    combined_list.append(pd.concat([blue_transposed, orange_transposed], axis=1))

combined_list[:5] # First 5 games in the dataset


#%% -----------Barcharts of Game Replay Metrics
'''
The following function generates barcharts that compare team statistics for a 
game
'''
def create_barcharts(game_num):
    for i, row in game_num.iterrows():
        # Create a new figure and axis
        fig, ax = plt.subplots()

        # Create a bar graph of the data for this row
        ax.bar(['Blue', 'Orange'], row.values, color=['blue','orange'])

        # Set the title and axis labels for this row
        ax.set_title(f"Stats for Row {i}")
        ax.set_xlabel('Team Color')
        ax.set_ylabel('Value')

        # Display the graph for this row
        plt.show()

# The first game
table = combined_list[0].iloc[:, 0:2]
table
game1 = combined_list[0].iloc[1:, 0:2]
create_barcharts(game1)

# The second game
table2 = combined_list[1].iloc[:, 0:2]
table2
game2 = combined_list[1].iloc[1:, 0:2]
create_barcharts(game2)

#%% -----------In-game player statistics
'''
Note: Data was only pulled from ranked (i.e. competetive), Rocket League
      does not allow 4 versus 4 in ranked matches, only in casual ones.
      Possibly implying 4v4 is more of a "party-game" mode. 
      Hence player_stats only have 3 columns
      
      Additionally, this will not work with data from a csv or conversion
      of columns into JSON dtype will be necessary.
      
The following function will return dataframes for player1, 2, and 3 depending
on the color of the player stats entered as a parameter.
'''
def get_player_data(df):
    # The following will provide a dataframe where each column is 
    # representative of a player
    players_stats = pd.json_normalize(df)

    # Extract the list of dictionaries from each column
    player1 = players_stats[0]
    player1 = pd.json_normalize(player1)
    if 'mvp' in player1:
        player1 = player1.drop('mvp', axis=1)

    
    player2 = players_stats[1]
    player2 = pd.json_normalize(player2)
    if 'mvp' in player2:
        player2 = player2.drop('mvp', axis=1)
    player2 = player2.add_prefix('p2_')
    

    
    player3 = players_stats[2]
    player3 = pd.json_normalize(player3)
    if 'mvp' in player3:
        player3 = player3.drop('mvp', axis=1)
    player3 = player3.add_prefix('p3_')
    
    return player1, player2, player3


# Blue players Data
blue_player_stats = team_stats.drop(team_stats.columns[52:], axis=1)
blue_players_stats = blue_player_stats['blue.players']
blue_players_stats

bp1, bp2, bp3 = get_player_data(blue_players_stats)

bp_combined = pd.concat([bp1, bp2, bp3], axis=1)

# Orange players Data
orange_player_stats = team_stats.drop(team_stats.columns[:52], axis=1)
orange_players_stats = orange_player_stats['orange.players']
orange_players_stats # get_player_data paramater

op1, op2, op3 = get_player_data(orange_players_stats)

op_combined = pd.concat([op1, op2, op3], axis=1)

#%% Dataframe with all players
# for the following dataframe 
# If there is no player 2, it implies the game-type was 1v1
# If there is no player 3, it implies the game-type was 1v1 or 2v2
all_players = pd.concat([bp_combined, op_combined], axis=0)

'''What car was used the most and by which teammate?'''
car_count = all_players[['car_name', 'p2_car_name', 'p3_car_name']].apply(pd.value_counts)


'''Who has played the most games?'''
name_count = all_players[['name', 'p2_name', 'p3_name']].apply(pd.value_counts)

name_count['total_games'] = name_count['name'] + name_count['p2_name'] + name_count['p3_name']

name_count = name_count.sort_values(by='total_games', ascending=False)

name_count.head(10)


#%%

# Load in the main dataset that will be worked on
replays_df = pd.read_csv('replays_data.csv')

# Load in the data individual matches for each team
blue_team = pd.read_csv('blue.csv')

orange_team = pd.read_csv('orange.csv')


# Blue players Data
blue_players_stats = blue_team['blue.players']

print(blue_players_stats.shape)
print(blue_players_stats)  # get player features


import ast
blue_player_stats = blue_team['blue.players']

# Start with an empty dataframe
all_players_df = pd.DataFrame()

# Define the desired unique keys
unique_keys = ['start_time', 'end_time', 'name', 'car_id', 'car_name',
               'shots', 'shots_against', 'goals', 'goals_against', 'saves', 'assists', 'score',
               'avg_distance_to_ball', 'avg_distance_to_ball_possession', 'avg_distance_to_ball_no_possession',
               'avg_distance_to_mates', 'time_defensive_third', 'time_neutral_third', 'time_offensive_third',
               'time_defensive_half', 'time_offensive_half', 'time_behind_ball', 'time_infront_ball',
               'time_most_back', 'time_most_forward', 'goals_against_while_last_defender', 'time_closest_to_ball',
               'time_farthest_from_ball', 'percent_defensive_third', 'percent_offensive_third',
               'percent_neutral_third', 'percent_defensive_half', 'percent_offensive_half',
               'percent_behind_ball', 'percent_infront_ball', 'percent_most_back', 'percent_most_forward',
               'percent_closest_to_ball', 'percent_farthest_from_ball', 'mvp']

# Iterate over the elements in the series
for i in range(len(blue_player_stats)):
    # Get the list of dictionaries for this match
    players_list = ast.literal_eval(blue_player_stats.iloc[i])
    
    # Limit the number of players to 3
    players_list = players_list[:3]
    
    # Create a dictionary to hold the player data for this game
    game_data = {key: [] for key in unique_keys}

    # Iterate over the players in the list
    for player_dict in players_list:
        # Append player data to the game_data dictionary
        for key in unique_keys:
            if key in ['shots', 'shots_against', 'goals', 'goals_against', 'saves', 'assists', 'score']:
                game_data[key].append(player_dict['stats']['core'].get(key, None))
            elif key in ['avg_distance_to_ball', 'avg_distance_to_ball_possession', 'avg_distance_to_ball_no_possession',
                         'avg_distance_to_mates', 'time_defensive_third', 'time_neutral_third', 'time_offensive_third',
                         'time_defensive_half', 'time_offensive_half', 'time_behind_ball', 'time_infront_ball',
                         'time_most_back', 'time_most_forward', 'goals_against_while_last_defender', 'time_closest_to_ball',
                         'time_farthest_from_ball', 'percent_defensive_third', 'percent_offensive_third',
                         'percent_neutral_third', 'percent_defensive_half', 'percent_offensive_half',
                         'percent_behind_ball', 'percent_infront_ball', 'percent_most_back', 'percent_most_forward',
                         'percent_closest_to_ball', 'percent_farthest_from_ball']:
                game_data[key].append(player_dict['stats']['positioning'].get(key, None))
            elif key == 'mvp':
                game_data[key].append(player_dict['stats']['core'].get(key, False))
            else:
                game_data[key].append(player_dict.get(key, None))
        
    # Create a DataFrame for this game
    game_df = pd.DataFrame(game_data)
    
    # If this is the first game we're adding, just assign the dataframe
    if all_players_df.empty:
        all_players_df = game_df
    # Otherwise, concatenate the new game to the existing dataframe
    else:
        all_players_df = pd.concat([all_players_df, game_df], ignore_index=True)

# Sort the dataframe by name in ascending order
all_players_df = all_players_df.sort_values(by='name', ascending=False)


# Print the sorted dataframe
print(all_players_df.head())
print("Important Note:")
print(" We only have to do this for the blue teams, since it provides us more than enough data to"
     " build a model. Teams for most games don't work like they would in a traditional setting."
     "Meaning that an individual can end up on any team with any player, assuming it isn't a 1 "
     "versus 1 match.")


all_players_df['goals_against_while_last_defender'] = all_players_df['goals_against_while_last_defender'].fillna(0)
all_players_df['goals_against_while_last_defender'] = all_players_df['goals_against_while_last_defender'].replace([None, ''], 0).astype(float)

import seaborn as sns
X = all_players_df[
    ['shots', 'shots_against', 'goals', 'goals_against', 'saves', 'assists', 
     'score', 'avg_distance_to_ball', 'avg_distance_to_ball_possession', 
     'avg_distance_to_ball_no_possession', 'avg_distance_to_mates',
    'time_defensive_third', 'time_neutral_third', 'time_offensive_third', 
     'time_defensive_half', 'time_offensive_half', 'time_behind_ball', 
     'time_infront_ball', 'time_most_back', 'time_most_forward',
    'goals_against_while_last_defender', 'time_closest_to_ball', 
     'time_farthest_from_ball', 'percent_defensive_third', 
     'percent_offensive_third', 'percent_neutral_third', 
     'percent_defensive_half', 'percent_offensive_half', 'percent_behind_ball', 
     'percent_infront_ball', 'percent_most_back', 'percent_most_forward', 
     'percent_closest_to_ball', 'percent_farthest_from_ball']
]

y = all_players_df['mvp']

from sklearn.preprocessing import LabelEncoder

# Create an instance of LabelEncoder
label_encoder = LabelEncoder()

# Fit and transform the target variable
y_encoded = label_encoder.fit_transform(y)

# Calculate the correlation matrix
correlation_matrix = X.corr()

# Print the correlation matrix
print(correlation_matrix)

# Plot the correlation matrix as a heatmap
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm")

# Show the plot
plt.show()