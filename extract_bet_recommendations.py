import pyodbc
import pandas as pd
from openpyxl import Workbook
from datetime import datetime

# Retrieve player data
risky_bet_query = """
--RISKY
    select 
Player
, case
	when goals > 2.5 then '3+ Goals'
	when goals > 1.7 then '2+ Goals'
	when goals > 0.8 then 'Anytime Goalscorer'
	else null
end as 'Risky Bet Recommendation'
, goals as bet_value
, team_name
, game_start_date_time
, venue
, home_team
, away_team


from bettingdata
where match_providerid = '{id}'
and prediction_type = 'Goals'
and goals > 0.8

union

select 
Player
, case
	when disposals >= 30 then '30+ Disposals'
	when disposals >= 25 then '25+ Disposals'
	when disposals >= 20 then '20+ Disposals'
	when disposals >= 15 then '15+ Disposals'
	else null
end as 'Risky Bet Recommendation'
, disposals  as bet_value
, team_name
, game_start_date_time
, venue
, home_team
, away_team


from bettingdata
where match_providerid = '{id}'
and prediction_type = 'Disposals'
and disposals >= 15

order by team_name, bet_value desc
;
    """
    
safe_bet_query = """
--SAFE
select 
Player
, case
	when disposals >= 32 then '30+ Disposals'
	when disposals >= 27 then '25+ Disposals'
	when disposals >= 22 then '20+ Disposals'
	when disposals >= 17 then '15+ Disposals'
	else null
end as 'Safe Bet Recommendation'
, disposals as bet_value
, team_name
, game_start_date_time
, venue
, home_team
, away_team


from bettingdata
where match_providerid = '{id}'
and prediction_type = 'Disposals'
and disposals >= 17

union all

select 
Player
, case
	when goals > 3.0 then '3+ Goals'
	when goals > 2.0 then '2+ Goals'
	when goals > 1.0 then 'Anytime Goalscorer'
	else null
end as 'Safe Bet Recommendation'
, goals as bet_value
, team_name
, game_start_date_time
, venue
, home_team
, away_team


from bettingdata
where match_providerid = '{id}'
and prediction_type = 'Goals'
and goals > 1.0

order by team_name, bet_value desc
"""

next_fixtures = """
	SELECT DISTINCT
    match_providerid,
	f.roundRoundNumber
FROM
    BettingData
INNER JOIN FutureMatchFixtures as f
on BettingData.match_providerid = f.providerId
and f.roundRoundNumber = {roundNumber}
WHERE
    game_start_date_time >= GETDATE() AND
    game_start_date_time <= DATEADD(DAY, 7, GETDATE())

"""

queries = [safe_bet_query, risky_bet_query]

def connect_to_mssql(database):
    
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};Server=JMSDESKTOPPC\SQLEXPRESS04;Database=' + database + ';Trusted_Connection=yes;'
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    return conn, cursor

def bet_recommendation_output(round_number: int):
    
    conn, cursor = connect_to_mssql('afl_project')
    
    next_fixtures_round = next_fixtures.replace('{roundNumber}', str(round_number))
    
    cursor.execute(next_fixtures_round)
    matches = cursor.fetchall()
    
    for query in queries:
        
        wb = Workbook()
        
        for match_id in matches:

            # Modify the SQL query with the current ID
            sql_query_with_id = query.replace('{id}', str(match_id[0]))
            
            # Execute the SQL query
            cursor.execute(sql_query_with_id)
            result_rows = cursor.fetchall()
            
            ws = wb.create_sheet(title=f'{result_rows[0][6].lower()} v {result_rows[0][7].lower()}')
            
            # Write the result to the worksheet
            ws.append(['Player', 'Bet Recommendation', 'Prediction', 'Player Team', 'Start Time', 'venue', 'home_team', 'away_team'])  # Add headers
            for row in result_rows:
                ws.append(list(row))
            
                # Auto-adjust column width to fit content
            for col in ws.columns:
                max_length = 0
                column = col[0].column_letter  # Get the column name
                for cell in col:
                    try:  # Necessary to avoid error on empty cells
                        if len(str(cell.value)) > max_length:
                            max_length = len(cell.value)
                    except:
                        pass
                adjusted_width = (max_length + 2) * 1.2  # Adjusted width based on content length
                ws.column_dimensions[column].width = adjusted_width
        
        wb.remove(wb['Sheet'])
        
        # Get the current date
        current_date = datetime.now()

        # Format the date as "dd_mm"
        dd_mm = current_date.strftime("%d_%m")
        
        if 'RISKY' in query:
            # Save the Excel file
            wb.save(f'Risky_Bets_Round_{matches[0][1]}_{dd_mm}.xlsx')
        else:
            wb.save(f'Safe_Bets_Round_{matches[0][1]}_{dd_mm}.xlsx')
    
