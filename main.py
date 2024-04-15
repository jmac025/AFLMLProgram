import disposals_ml
import get_data
import goals_ml
import extract_bet_recommendations as bet

TRAIN_MODELS = False
TEST_MODELS = False
#Refresh the data from afltables

if TEST_MODELS:
    
    disposals_ml.test_models()
    goals_ml.test_models()
    
if not TRAIN_MODELS:
    # get_data.main()

    # get_data.run_sql_file('C:\\Users\\Jordan\\Documents\\SQL Server Management Studio\\AFL\\Create and Load Dim Player Table.sql')
    # get_data.run_sql_file('C:\\Users\\Jordan\\Documents\\SQL Server Management Studio\\AFL\\Update Stadium Names.sql')
    # get_data.run_sql_file('C:\\Users\\Jordan\\Documents\\SQL Server Management Studio\\AFL\\Create Match Summary Table.sql')
    # get_data.run_sql_file('C:\\Users\\Jordan\\Documents\\SQL Server Management Studio\\AFL\\Create ML_Player_Data table.sql')
    
    disposals_ml.run_disposals_model()
    goals_ml.run_goals_model()

    get_data.run_sql_file('C:\\Users\\Jordan\\Documents\\SQL Server Management Studio\\AFL\\Create Combined Prediction Mart.sql')
    get_data.run_sql_file('C:\\Users\\Jordan\\Documents\\SQL Server Management Studio\\AFL\\Create Betting Data Table.sql')

    bet.bet_recommendation_output(6)
    
else:
    disposals_ml.train_model()
    goals_ml.train_model()
    
