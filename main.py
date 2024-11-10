import requests
import sqlite3
import pandas as pd

from database import teams

connection = sqlite3.connect('nhl_database.db')
base_url = 'https://api-web.nhle.com/v1/'
nhle_stats_base_url = 'https://api.nhle.com/stats/rest/en'

def fetch_data(url):
    response = requests.get(url)
    # Check if the request was successful
    response.raise_for_status()  # Raises an HTTPError if the response status is 4xx or 5xx
    return response.json()  # Return the JSON response if successful

def pull_team_season_gametypes(team_abbreviation):
    url=f"https://api-web.nhle.com/v1/club-stats-season/{team_abbreviation}"
    response = requests.get(url)
    data = response.json()
    return pd.DataFrame(data)

def pull_team_season_roster_performance(base_url, team_abbr, season, gametype):
    url= base_url+f'club-stats/{team_abbr}/{season}/{gametype}'
    data = fetch_data(url)
    skater_data = data["skaters"]
    flattened_data = []
    for skater in skater_data:
        flattened_data.append({
            "playerId": skater["playerId"],
            "headshot": skater["headshot"],
            "firstName": skater["firstName"]["default"],
            "lastName": skater["lastName"]["default"],
            "positionCode": skater["positionCode"],
            "gamesPlayed": skater["gamesPlayed"],
            "goals": skater["goals"],
            "assists": skater["assists"],
            "points": skater["points"],
            "plusMinus": skater["plusMinus"],
            "penaltyMinutes": skater["penaltyMinutes"],
            "powerPlayGoals": skater["powerPlayGoals"],
            "shorthandedGoals": skater["shorthandedGoals"],
            "gameWinningGoals": skater["gameWinningGoals"],
            "overtimeGoals": skater["overtimeGoals"],
            "shots": skater["shots"],
            "shootingPctg": skater["shootingPctg"]
        })
    team_data_df = pd.DataFrame(flattened_data)
    return team_data_df

def pull_team_abbreviations(connection, nhle_stats_base_url):
    db_connection = connection
    url=f"{nhle_stats_base_url}/team"
    data = fetch_data(url)
    dataframe = pd.DataFrame(data['data'])
    dataframe = dataframe.dropna()
    dataframe = dataframe.astype({'id':'int', 'franchiseId':'int', 'fullName':'string', 'leagueId':'int', 'rawTricode':'string', 'triCode':'string'})
    dataframe = dataframe.rename(columns={
        "id":"team_id",
        "franchiseId":"franchise_id",
        "fullName":"team_name",
        "triCode":"name_code"
    })
    dataframe = dataframe[['team_id','franchise_id','team_name','name_code']].copy()
    return dataframe


# noinspection PyTypeChecker
def upload_df(dataframe: pd.DataFrame, connection, table_name: str, mode = 'replace'):
    df = dataframe
    db = connection
    table = table_name
    mode_choice = mode
    df.to_sql(table,db,index = False,if_exists=mode_choice)

def pull_season_stats(nhle_stats_base_url) -> pd.DataFrame:
    url = f"{nhle_stats_base_url}/season"
    data = fetch_data(url)
    dataframe = pd.DataFrame(data['data'])
    dataframe = dataframe.rename(columns={
        "id": "season_id",
        "allStarGameInUse": "all_star_game_in_use",
        "conferencesInUse": "conferences_in_use",
        "divisionsInUse": "divisions_in_use",
        "endDate": "end_date",
        "entryDraftInUse": "entry_draft_in_use",
        "formattedSeasonId": "formatted_season_id",
        "minimumPlayoffMinutesForGoalieStatsLeaders": "minimum_playoff_minutes_for_goalie_stats_leaders",
        "minimumRegularGamesForGoalieStatsLeaders": "minimum_regular_games_for_goalie_stats_leaders",
        "nhlStanleyCupOwner": "nhl_stanley_cup_owner",
        "numberOfGames": "number_of_games",
        "olympicsParticipation": "olympics_participation",
        "pointForOTLossInUse": "point_for_ot_loss_in_use",
        "preseasonStartdate": "preseason_start_date",
        "regularSeasonEndDate": "regular_season_end_date",
        "rowInUse": "row_in_use",
        "seasonOrdinal": "season_ordinal",
        "startDate": "start_date",
        "supplementalDraftInUse": "supplemental_draft_in_use",
        "tiesInUse": "ties_in_use",
        "totalPlayoffGames": "total_playoff_games",
        "totalRegularSeasonGames": "total_regular_season_games",
        "wildcardInUse": "wildcard_in_use"
    })
    print(dataframe.columns)
    return dataframe

pull_season_stats(nhle_stats_base_url)
def pull_current_team_abbreviations(connection) -> pd.DataFrame:
    db_connection = connection
    cursor = connection.cursor()
    cursor.execute("SELECT name_code, team_id FROM teams")
    results = [(row[0],row[1]) for row in cursor.fetchall()]
    return pd.DataFrame(results)

if __name__ == '__main__':
    team_abbreviations_df = pull_team_abbreviations(connection, nhle_stats_base_url)
    upload_df(team_abbreviations_df,connection,'teams', mode='append')
    #Teams three letter team name as well as the teams ID for joining
    team_abbr_and_id_df = pull_current_team_abbreviations(connection)
    season_stats_df = pull_season_stats(nhle_stats_base_url)
    print(season_stats_df)
    upload_df(season_stats_df,connection,'overall_season_info',mode='append')
#    for _,row in team_abbr_and_id_df.iterrows():
#        game_types_played_df = pull_team_season_gametypes(row[0])
#        for season in game_types_played_df['season']:
#
#            season_roster_performance = pull_team_season_roster_performance(base_url=base_url, team_abbr=teams, season=season, gametype=2)





