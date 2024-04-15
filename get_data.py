import rpy2.robjects as robjects
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
import pandas as pd
import pyodbc
from datetime import datetime
import pytz



def generate_mssql_script(table_name, column_names):
    # Generate MSSQL script for creating a table
    script = f"CREATE TABLE {table_name} (\n"
    for column in column_names:
        script += f"    {column} NVARCHAR(MAX),\n"
    script = script.rstrip(",\n") + "\n);"
    return script

def insert_match_stats(year, competition, table_name, column_names, concludedBool):

    # Import the fitzRoy package
    base = importr("fitzRoy")

    # Activate pandas conversion
    pandas2ri.activate()

    # Call the fetch_fixture function and convert the result to a pandas DataFrame
    fixture_data = base.fetch_fixture(year, comp=competition)
    fixture_df = pandas2ri.DataFrame(fixture_data)

    # Connect to MSSQL
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};Server=JMSDESKTOPPC\SQLEXPRESS04;Database=afl_project;Trusted_Connection=yes;'
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    # Create a list to store dictionaries
    data_list = []
    rows = fixture_df.nrow
    # Loop through the rows and add data to the list
    for i in range(fixture_df.nrow):
        row = fixture_df.rx(i + 1, True)
        row_dict = {}
        for column, value in zip(fixture_df.colnames, row):
            row_dict[column] = value[0]
        data_list.append(row_dict)

    
    cursor.execute("select distinct providerid from matchresults")
    result = cursor.fetchall()
    
    match_ids = [tup[0] for tup in result]
    
    if not concludedBool:
        
        cursor.execute("delete from futurematchfixtures")
        conn.commit()
    
    
    for row in data_list:
        
        if concludedBool:
            
            if row['providerId'] not in match_ids:
            
                if row['status'] == 'CONCLUDED':
                    if 'round.byes' in row:
                        row.pop("round.byes")
                    if 'metadata.travel_link' in row:
                        row.pop("metadata.travel_link")
                    if 'metadata.ticket_link' in row:
                        row.pop("metadata.ticket_link")
                    if 'round.utcStartTime' in row:
                        row.pop("round.utcStartTime")
                    if 'round.utcEndTime' in row:
                        row.pop("round.utcEndTime")
                    
                    utc_time = datetime.strptime(row["utcStartTime"], "%Y-%m-%dT%H:%M:%S.%f%z")
                    utc_timezone = pytz.timezone('UTC')
                    brisbane_timezone = pytz.timezone('Australia/Brisbane')
                    brisbane_time = utc_time.astimezone(brisbane_timezone)

                    # Format the result for MSSQL insertion (e.g., '2021-03-18 08:25:00')
                    formatted_result = brisbane_time.strftime('%Y-%m-%d %H:%M:%S')

                    row["utcStartTime"] = formatted_result

                    # Explicitly specify columns in the INSERT query
                    columns = ', '.join(column_names)
                    values = ', '.join(['?' for _ in row.values()])
                    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

                    print(insert_query, tuple(row.values()))

                    # Execute the parameterized query
                    cursor.execute(insert_query, tuple(row.values()))
                    conn.commit()
        
        else:
            
            current_datetime = datetime.now()
            current_year = current_datetime.year
            date_object = datetime.strptime(row['utcStartTime'], '%Y-%m-%dT%H:%M:%S.%f%z')
            game_year = date_object.year
            
            if game_year == current_year :
                if 'round.byes' in row:
                    row.pop("round.byes")
                if 'metadata.travel_link' in row:
                    row.pop("metadata.travel_link")
                if 'metadata.ticket_link' in row:
                    row.pop("metadata.ticket_link")
                if 'round.utcStartTime' in row:
                    row.pop("round.utcStartTime")
                if 'round.utcEndTime' in row:
                    row.pop("round.utcEndTime")
                
                utc_time = datetime.strptime(row["utcStartTime"], "%Y-%m-%dT%H:%M:%S.%f%z")
                utc_timezone = pytz.timezone('UTC')
                brisbane_timezone = pytz.timezone('Australia/Brisbane')
                brisbane_time = utc_time.astimezone(brisbane_timezone)

                # Format the result for MSSQL insertion (e.g., '2021-03-18 08:25:00')
                formatted_result = brisbane_time.strftime('%Y-%m-%d %H:%M:%S')

                row["utcStartTime"] = formatted_result

                # Explicitly specify columns in the INSERT query
                columns = ', '.join(column_names)
                values = ', '.join(['?' for _ in row.values()])
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

                print(insert_query, tuple(row.values()))

                # Execute the parameterized query
                cursor.execute(insert_query, tuple(row.values()))
                conn.commit()
    
    cursor.close()
    conn.close()    
    print(f"Data inserted into MSSQL table successfully.")

def insert_player_stats(year, competition, table_name, column_names):

    # Import the fitzRoy package
    base = importr("fitzRoy")

    # Activate pandas conversion
    pandas2ri.activate()

    # Call the fetch_fixture function and convert the result to a pandas DataFrame
    fixture_data = base.fetch_player_stats_afl(season=year,round_number=robjects.r('NULL'),comp=competition)
    fixture_df = pandas2ri.DataFrame(fixture_data)

    # Connect to MSSQL
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};Server=JMSDESKTOPPC\SQLEXPRESS04;Database=afl_project;Trusted_Connection=yes;'
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    # Create a list to store dictionaries
    data_list = []
    rows = fixture_df.nrow
    # Loop through the rows and add data to the list
    for i in range(fixture_df.nrow):
        row = fixture_df.rx(i + 1, True)
        row_dict = {}
        for column, value in zip(fixture_df.colnames, row):
            if str(value[0]) in ('nan', 'NA'):
                row_dict[column] = 0.0
            else:
                row_dict[column] = value[0]
        data_list.append(row_dict)

    cursor.execute("select distinct providerid from playerstats")
    result = cursor.fetchall()
    
    match_ids = [tup[0] for tup in result]
    
    for row in data_list:
        
        if row['providerId'] not in match_ids:

            if row['status'] == 'CONCLUDED':
                utc_time = datetime.strptime(row["utcStartTime"], "%Y-%m-%dT%H:%M:%S.%f%z")
                utc_timezone = pytz.timezone('UTC')
                brisbane_timezone = pytz.timezone('Australia/Brisbane')
                brisbane_time = utc_time.astimezone(brisbane_timezone)

                # Format the result for MSSQL insertion (e.g., '2021-03-18 08:25:00')
                formatted_result = brisbane_time.strftime('%Y-%m-%d %H:%M:%S')

                row["utcStartTime"] = formatted_result
                if row["player.photoURL"] is not None:
                    row["player.photoURL"] = row["player.photoURL"].split(".png")[0] + ".png"
                else:
                    row["player.photoURL"] = ''
                    
                row["player.player.player.surname"] = row["player.player.player.surname"].replace("'","")

                # Explicitly specify columns in the INSERT query
                columns = ', '.join(column_names)
                fields = f"""'{row['providerId']}', '{row['utcStartTime']}', {row['round.roundNumber']}, '{row['venue.name']}',
                '{row['home.team.name']}', '{row['away.team.name']}', '{row['player.player.position']}', 
                '{row['player.player.player.playerId']}', {row['player.player.player.playerJumperNumber']}, '{row['player.player.player.givenName']}', '{row['player.player.player.surname']}', 
                '{row['player.photoURL']}', '{row['teamId']}', {row['timeOnGroundPercentage']}, {row['goals']}, {row['behinds']}, {row['kicks']}, 
                {row['handballs']}, {row['disposals']}, {row['marks']}, {row['bounces']}, {row['tackles']}, {row['contestedPossessions']}, 
                {row['uncontestedPossessions']}, {row['totalPossessions']}, {row['inside50s']}, {row['marksInside50']}, 
                {row['contestedMarks']}, {row['hitouts']}, {row['onePercenters']}, {row['disposalEfficiency']}, {row['clangers']}, 
                {row['freesFor']}, {row['freesAgainst']}, {row['dreamTeamPoints']}, {row['rebound50s']}, {row['goalAssists']}, 
                {row['goalAccuracy']}, {row['ratingPoints']}, {row['turnovers']}, {row['intercepts']}, {row['tacklesInside50']}, 
                {row['shotsAtGoal']}, {row['goalEfficiency']}, {row['shotEfficiency']}, {row['interchangeCounts']}, {row['scoreInvolvements']}, 
                {row['metresGained']}, {row['clearances.centreClearances']}, {row['clearances.stoppageClearances']}, {row['clearances.totalClearances']}, {row['extendedStats.effectiveKicks']}, 
                {row['extendedStats.kickEfficiency']}, {row['extendedStats.kickToHandballRatio']}, {row['extendedStats.effectiveDisposals']}, {row['extendedStats.marksOnLead']}, {row['extendedStats.interceptMarks']}, 
                {row['extendedStats.contestedPossessionRate']}, {row['extendedStats.hitoutsToAdvantage']}, {row['extendedStats.hitoutWinPercentage']}, {row['extendedStats.hitoutToAdvantageRate']}, {row['extendedStats.groundBallGets']}, 
                {row['extendedStats.f50GroundBallGets']}, {row['extendedStats.scoreLaunches']}, {row['extendedStats.pressureActs']}, {row['extendedStats.defHalfPressureActs']}, {row['extendedStats.spoils']}, {row['extendedStats.ruckContests']}, 
                {row['extendedStats.contestDefOneOnOnes']}, {row['extendedStats.contestDefLosses']}, {row['extendedStats.contestDefLossPercentage']}, {row['extendedStats.contestOffOneOnOnes']}, {row['extendedStats.contestOffWins']}, {row['extendedStats.contestOffWinsPercentage']}, 
                {row['extendedStats.centreBounceAttendances']}, {row['extendedStats.kickins']}, {row['extendedStats.kickinsPlayon']}"""
                
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({fields})"

                #print(insert_query)
                        
                # Execute the parameterized query
                cursor.execute(insert_query)
                conn.commit()


    cursor.close()
    conn.close()  
    print(f"Data inserted into MSSQL table successfully.")       

def run_sql_file(file_path):

    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};Server=JMSDESKTOPPC\SQLEXPRESS04;Database=afl_project;Trusted_Connection=yes;'
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    with open(file_path, 'r') as file:
        sql_script = file.read()
    
    # Split SQL script into individual statements
    sql_statements = sql_script.split(';')

    # Execute each statement
    for statement in sql_statements:
        if statement.strip():  # Check if the statement is not empty
            cursor.execute(statement)

    conn.commit()
    
    
def main():
    # List of column names
    match_column_names = [
    'id', 'providerId', 'gameStartDateTime', 'status', 'compSeasonid', 'compSeasonproviderId',
    'compSeasonname', 'compSeasonshortName', 'compSeasoncurrentRoundNumber', 'roundid',
    'roundproviderId', 'roundabbreviation', 'roundname', 'roundroundNumber',
    'hometeamid', 'hometeamproviderId',
    'hometeamname', 'hometeamabbreviation', 'hometeamnickname', 'hometeamteamType',
    'hometeamclubid', 'hometeamclubproviderId', 'hometeamclubname',
    'hometeamclubabbreviation', 'hometeamclubnickname', 'homescoregoals',
    'homescorebehinds', 'homescoretotalScore', 'homescoresuperGoals', 'awayteamid',
    'awayteamproviderId', 'awayteamname', 'awayteamabbreviation', 'awayteamnickname',
    'awayteamteamType', 'awayteamclubid', 'awayteamclubproviderId', 'awayteamclubname',
    'awayteamclubabbreviation', 'awayteamclubnickname', 'awayscoregoals',
    'awayscorebehinds', 'awayscoretotalScore', 'awayscoresuperGoals', 'venueid',
    'venueproviderId', 'venuename', 'venueabbreviation', 'venuelocation', 'venuestate',
    'venuetimezone', 'venuelandOwner','compSeasonyear']

    stat_column_names = ['providerId', 'utcStartTime', 'roundNumber', 'venue_name', 
                     'home_team_name', 'away_team_name', 
                     'player_position', 'playerId', 'player_JumperNumber', 
                     'player_givenName', 'player_surname', 'player_photoURL', 'teamId', 'timeOnGroundPercentage', 'goals', 'behinds', 
                     'kicks', 'handballs', 'disposals', 'marks', 'bounces', 'tackles', 'contestedPossessions', 'uncontestedPossessions', 
                     'totalPossessions', 'inside50s', 'marksInside50', 'contestedMarks', 'hitouts', 'onePercenters', 'disposalEfficiency', 'clangers', 
                     'freesFor', 'freesAgainst', 'dreamTeamPoints', 'rebound50s', 'goalAssists', 'goalAccuracy', 'ratingPoints', 
                     'turnovers', 'intercepts', 'tacklesInside50', 'shotsAtGoal', 'goalEfficiency', 'shotEfficiency', 'interchangeCounts', 'scoreInvolvements', 
                     'metresGained', 'centreClearances', 'stoppageClearances', 'totalClearances', 'effectiveKicks', 
                     'kickEfficiency', 'kickToHandballRatio', 'effectiveDisposals', 'marksOnLead', 
                     'interceptMarks', 'contestedPossessionRate', 'hitoutsToAdvantage', 'hitoutWinPercentage', 
                     'hitoutToAdvantageRate', 'groundBallGets', 'f50GroundBallGets', 'scoreLaunches', 
                     'pressureActs', 'defHalfPressureActs', 'spoils', 'ruckContests', 'contestDefOneOnOnes', 
                     'contestDefLosses', 'contestDefLossPercentage', 'contestOffOneOnOnes', 'contestOffWins', 
                     'contestOffWinsPercentage', 'centreBounceAttendances', 'kickins', 'kickinsPlayon']
    
    # Example usage
    comp = "AFLM"
    match = "MatchResults"
    matchFixture = "FutureMatchFixtures"
    stats = "PlayerStats"
    season_list = [2024]

    for year in season_list:
        insert_match_stats(year, comp, match, match_column_names, True)
        insert_match_stats(year, comp, matchFixture, match_column_names, False)
        insert_player_stats(year, comp, stats, stat_column_names)
        
