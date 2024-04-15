from oddsapi import OddsApiClient
from asyncio import gather
import argparse
import requests

API_KEY = '18adf49f718b06b23cb6a6d606d5c404'
sports_response = requests.get('https://api.the-odds-api.com/v4/sports', params={
    'api_key': API_KEY
})

SPORT = 'aussierules_afl'
REGIONS = 'au'
MARKETS = 'player_goal_scorer_anytime'
ODDS_FORMAT = 'decimal'
DATE_FORMAT = 'iso'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
#
# Now get a list of live & upcoming games for the sport you want, along with odds for different bookmakers
# This will deduct from the usage quota
# The usage quota cost = [number of markets specified] x [number of regions specified]
# For examples of usage quota costs, see https://the-odds-api.com/liveapi/guides/v4/#usage-quota-costs
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

odds_response = requests.get(f'https://api.the-odds-api.com/v4/sports/au/players/disposals', params={
    'api_key': API_KEY,
    'oddsFormat': ODDS_FORMAT,
    'dateFormat': DATE_FORMAT
})

if odds_response.status_code != 200:
    print(f'Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}')

else:
    odds_json = odds_response.json()
    print('Number of events:', len(odds_json))
    print(odds_json)

    # Check the usage quota
    print('Remaining requests', odds_response.headers['x-requests-remaining'])
    print('Used requests', odds_response.headers['x-requests-used'])