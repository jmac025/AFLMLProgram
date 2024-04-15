import pyodbc
import matplotlib.pyplot as plt
import pandas as pd
import pylab as pl
import numpy as np
from datetime import datetime
import time
import pytz
import pickle
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt
from statsmodels.stats.outliers_influence import OLSInfluence
import numpy as np


def connect_to_mssql(database):
    
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};Server=JMSDESKTOPPC\SQLEXPRESS04;Database=' + database + ';Trusted_Connection=yes;'
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    return conn, cursor
    
def get_player_data():
    
    conn, cursor = connect_to_mssql('afl_project')
    
    # Retrieve player data
    get_data_query = "Select upper(player_name) as player_name, upper(venuename) as venuename, disposals, goals, upper(opposition) as opposition, year(utcstarttime) as match_year from ML_Player_Data"
    
    cursor.execute(get_data_query)
    result = cursor.fetchall()
    
    df = pd.DataFrame([tuple(row) for row in result], columns=[desc[0] for desc in cursor.description])
    
    return df

def train_goals_model(data, cutoff_year, history_estimators, recent_estimators, testing: bool):
    # Filter data based on cutoff year
    historical_data = data[data['match_year'] < cutoff_year]
    recent_data = data[data['match_year'] >= cutoff_year]
    
    # Training on historical data
    ct_hist = ColumnTransformer(transformers=[('encoder', OneHotEncoder(handle_unknown='ignore'), ['player_name', 'venuename', 'opposition', 'match_year'])], remainder='passthrough')
    X_hist = ct_hist.fit_transform(historical_data.drop(columns=['goals']))
    y_hist = historical_data['goals']
    
    print(f"Training on Historical Data........... {datetime.now()}")
    hist_start_time = time.time()
    regressor_hist = RandomForestRegressor(n_estimators=history_estimators, random_state=42)
    regressor_hist.fit(X_hist, y_hist)
    hist_end_time = time.time()
    history_duration = hist_end_time - hist_start_time
    print(f"Training on Historical Data Completed. {datetime.now()}")
    
    # Testing on historical data
    print(f"Testing on Historical Data........... {datetime.now()}")
    X_hist_test = ct_hist.transform(historical_data.drop(columns=['goals']))
    y_hist_pred = regressor_hist.predict(X_hist_test)
    
    # Training on recent data
    ct_recent = ColumnTransformer(transformers=[('encoder', OneHotEncoder(handle_unknown='ignore'), ['player_name'])], remainder='passthrough')
    X_recent = ct_recent.fit_transform(recent_data.drop(columns=['goals', 'venuename', 'opposition', 'match_year']))
    y_recent = recent_data['goals']
    
    print(f"Training on Recent Data........... {datetime.now()}")
    recent_start_time = time.time()
    regressor_recent = RandomForestRegressor(n_estimators=recent_estimators, random_state=42)
    regressor_recent.fit(X_recent, y_recent)
    recent_end_time = time.time()
    recent_duration = recent_end_time - recent_start_time
    print(f"Training on Recent Data Completed. {datetime.now()}")
    
    # Testing on recent data
    print(f"Testing on Recent Data........... {datetime.now()}")
    X_recent_test = ct_recent.transform(recent_data.drop(columns=['goals', 'venuename', 'opposition', 'match_year']))
    y_recent_pred = regressor_recent.predict(X_recent_test)
    
    if testing:
        epsilon = 1e-10
        
        #Testing calculations
        mse_hist = mean_squared_error(y_hist, y_hist_pred)
        mae_hist = mean_absolute_error(y_hist, y_hist_pred)
        rmse_hist = np.sqrt(mean_squared_error(y_hist, y_hist_pred))
        mape_hist = np.mean(np.abs((y_hist - y_hist_pred) / (y_hist + epsilon))) * 100
        r2_hist = r2_score(y_hist, y_hist_pred)
        print(f"MSE on Historical Data: {mse_hist}")
        print(f"MAE on Historical Data: {mae_hist}")
        print(f"RMSE on Historical Data: {rmse_hist}")
        print(f"R-squared on Historical Data: {r2_hist}")
        print(f"MAPE on Historical Data: {mape_hist}")
        print(f"Testing on Historical Data Completed. {datetime.now()}")
        
        #Testing calculations
        mse_recent = mean_squared_error(y_recent, y_recent_pred)
        mae_recent = mean_absolute_error(y_recent, y_recent_pred)
        rmse_recent = np.sqrt(mean_squared_error(y_recent, y_recent_pred))
        mape_recent = np.mean(np.abs((y_recent - y_recent_pred) / (y_recent + epsilon))) * 100
        r2_recent = r2_score(y_recent, y_recent_pred)
        print(f"N_Estimators value: {recent_estimators}")
        print(f"MSE on Recent Data: {mse_recent}")
        print(f"MAE on Recent Data: {mae_recent}")
        print(f"RMSE on Recent Data: {rmse_recent}")
        print(f"R-squared on Recent Data: {r2_recent}")
        print(f"MAPE on Recent Data: {mape_recent}")
        print(f"Testing on Recent Data Completed. {datetime.now()}")
        
        conn, cursor = connect_to_mssql('afl_project')
        
        # Insert statement for historical data
        insert_hist_query = """
        INSERT INTO model_performance (estimators, model_type, mse, mae, rmse, r2, mape, run_duration, load_datetime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_hist_query, history_estimators, 'Goals History', mse_hist, mae_hist, rmse_hist, r2_hist, mape_hist, history_duration, datetime.now())
        conn.commit()

        # Insert statement for recent data
        insert_recent_query = """
        INSERT INTO model_performance (estimators, model_type, mse, mae, rmse, r2, mape, run_duration, load_datetime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_recent_query, recent_estimators, 'Goals Recent', mse_recent, mae_recent, rmse_recent, r2_recent, mape_recent, recent_duration, datetime.now())
        conn.commit()
    
    else:
        # Combine both models into one
        combined_model = combine_models(regressor_hist, regressor_recent, ct_hist, ct_recent)
        
         # Save the combined model and transformers to disk using pickle
        with open('goals_combined_model.pkl', 'wb') as f:
            pickle.dump((combined_model, ct_hist, ct_recent), f)
        print("Disposals Model saved successfully!")

class CombinedModel:
    def __init__(self, model1, model2, ct1, ct2):
        self.model1 = model1
        self.model2 = model2
        self.ct1 = ct1
        self.ct2 = ct2
    
    def predict(self, X):
        # Transform input data
        X_hist = self.ct1.transform(X)
        X_recent = self.ct2.transform(X)
        
        # Predictions from historical model
        pred_hist = self.model1.predict(X_hist)
        
        # Predictions from recent model
        pred_recent = self.model2.predict(X_recent)
        
        # Combine predictions
        combined_pred = (pred_hist + pred_recent) / 2.0
        
        return combined_pred

def combine_models(model1, model2, ct1, ct2):
    # Return an instance of the CombinedModel class
    return CombinedModel(model1, model2, ct1, ct2)    

def get_player_details():
    
    conn, cursor = connect_to_mssql('afl_project')
    
    # Retrieve player data
    get_data_query = """with future_fixtures as (
select distinct concat(upper(player_givenname), ' ', upper(player_surname)) as player_name
, upper(venuename) as venuename
, case
	when hometeamproviderid = plyr.teamproviderId then upper(awayteamname)
	else upper(hometeamname)
	end as opposition
from dim_player as plyr
left join (
			select 
			playerid
			, avg(timeongroundpercentage) as timeongroundpercentage
            , avg(disposals) as disposals
			from ml_player_data
			group by playerid
			) as a
on a.playerid = plyr.playerid
inner join (select venuename
, hometeamproviderid
, awayteamname
, hometeamname
from futurematchfixtures) as b
on 1=1
where season = 2024
)

select f.*
, pred.disposals
, 2024 match_year
from future_fixtures as f
left join (
select player, venue, opponent, Disposals, load_date 
from [dbo].[PredictionData_Disposals]
where load_date = (select max(load_date) from predictiondata_disposals)
) as pred
on f.player_name = pred.Player
and f.venuename = pred.Venue
and f.opposition = pred.Opponent;"""
    
    cursor.execute(get_data_query)
    result = cursor.fetchall()
    
    df = pd.DataFrame([tuple(row) for row in result], columns=[desc[0] for desc in cursor.description])
    
    return df

def test_models():
    # Train the model
    train_goals_model(get_player_data(), 2024, 50, 50, True)
    train_goals_model(get_player_data(), 2024, 100, 100, True)
    train_goals_model(get_player_data(), 2024, 150, 150, True)
    train_goals_model(get_player_data(), 2024, 200, 250, True)
    train_goals_model(get_player_data(), 2024, 50, 300, True)

def train_model():
    train_goals_model(get_player_data(), 2023, 200, 300, False)
            
def run_goals_model():
    
    # Load the combined model and transformers from disk using pickle
    with open('goals_combined_model.pkl', 'rb') as f:
        combined_model, ct_hist, ct_recent = pickle.load(f)
        
    player_data = get_player_details()

    # Extracting columns used for prediction
    hist_cols = ['player_name', 'venuename', 'opposition', 'disposals', 'match_year']
    recent_cols = ['player_name', 'disposals']

    # Transform player_data to match the format expected by the model
    X_hist = ct_hist.transform(player_data[hist_cols])
    X_recent = ct_recent.transform(player_data[recent_cols])

    # Predict disposals using the combined model
    predictions_hist = combined_model.model1.predict(X_hist)
    predictions_recent = combined_model.model2.predict(X_recent)
    predictions = (predictions_hist + predictions_recent) / 2.0
    predictions = np.round(predictions, 2)
    
    conn, cursor = connect_to_mssql('afl_project')
    
    brisbane_timezone = pytz.timezone('Australia/Brisbane')
    brisbane_time = datetime.now(brisbane_timezone)
    formatted_result = brisbane_time.strftime('%Y-%m-%d %H:%M:%S')
    
    # Insert data from the DataFrame into the table
    for idx, row in enumerate(player_data.itertuples(index=False)):
        insert_query = """
        INSERT INTO predictiondata_goals (Player, Venue, Opponent, TimeOnGroundPercentage, Disposals, Goals, Load_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, row.player_name, row.venuename, row.opposition, 0, row.disposals, predictions[idx], formatted_result)
        conn.commit()
        
    print("Goals run completed")
