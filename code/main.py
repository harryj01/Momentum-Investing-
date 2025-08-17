import pandas as pd
import numpy as np
import yfinance as yf
from datetime import timedelta
from datetime import datetime
from yahooquery import search as yq_search
import random
import math
import time
import os
import subprocess
from colorama import Fore, Style, init
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from threading import Lock
import contextlib
import io

print_lock = Lock()
init(autoreset=True)
pd.set_option('mode.chained_assignment', None)


print("Preprocessing data ...")
data_sheets_path = r"/Users/priyaanksheth/Downloads/Momentum/Data_Sheets"
#region     This is dfs work
filtered_components = pd.read_csv(os.path.join(data_sheets_path,"Filtered_index_components.csv"))
df2 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-2 (2023).csv'))
df3 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-3 (HY2 2022).csv'), low_memory=False)
df4 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-4 (HY1 2022).csv'))
df5 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-5 (HY2 2021).csv'))
df6 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-6 (HY1 2021).csv'))
df7 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-7 (HY2 2022).csv'))
df8 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-8 (HY1 2020).csv'))
df9 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-9 (HY2 2019).csv'))
df10 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-10 (Hy1 2019).csv'))
df11 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-11 (HY2 2018).csv'))
df12 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-12 (HY1 2018).csv'))
df13 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-13 (HY2 2017).csv'))
df14 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-14 (HY1 2017).csv'))
df15 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-15 (HY2 2016).csv'))
df16 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-16 (HY1 2016).csv'))
df17 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-17 (HY2 2015).csv'))
df18 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-18 (HY1 2015).csv'))
df19 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-19 (HY2 2014).csv'))
df20 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-20 (HY1 2014).csv'))
df21 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-21 (HY2 2013).csv'))
df22 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-22 (HY1 2013).csv'))
df23 = pd.read_csv(os.path.join(data_sheets_path, 'Pull-23 (HY2 2012).csv'))
answer = pd.read_csv(os.path.join(data_sheets_path, 'Sample NAV.csv'))

for i in range(2, 24):
    df_name = 'df' + str(i)
    
    # Drop NaN values
    globals()[df_name] = globals()[df_name].dropna()
    
    # Set columns based on the first row
    globals()[df_name].columns = globals()[df_name].iloc[0]
    
    # Drop the first row
    globals()[df_name] = globals()[df_name].drop(globals()[df_name].index[0])
    
    # Reset index
    globals()[df_name] = globals()[df_name].reset_index(drop=True)

filtered_components = filtered_components.drop(columns="Unnamed: 0")
filtered_components['NDP_Date'] = pd.to_datetime(filtered_components['NDP_Date'])

# all_data  = pd.concat([df2, df3, df4, df5, df6, df7, df8, df9, df10,df11, df12, df13, df14, df15, df16, df17, df18, df19, df20, df21 ,df22, df23], axis=0, ignore_index=False)
all_data = pd.concat([globals()[f"df{i}"] for i in range(2, 24)], axis=0, ignore_index=False)

columns_to_drop = ['CD_Sector', 'Accord Code']
all_data = all_data.drop(columns=columns_to_drop)

for i in range(2, 24):
    var_name = f"df{i}"
    if var_name in globals():
        del globals()[var_name]

#endregion

#region     This gets the list of trading dates for the strategy to run
def get_all_mondays(start_year, end_year):

    all_mondays = []

    # Set the start date to the first Monday of the start year
    current_date = datetime(start_year, 10, 7)
    current_date += timedelta(days=(7 - current_date.weekday()) % 7)

    # Iterate through weeks until the end of the end year
    while current_date.year <= end_year:
        all_mondays.append(current_date)
        current_date += timedelta(weeks=1)

    return all_mondays


start_year_for_lookback = 2012
start_year_for_holding = 2013
end_year = 2022
many_mondays = get_all_mondays(start_year_for_lookback, end_year)
mondays_array = get_all_mondays(start_year_for_holding, end_year)


from datetime import datetime

dates_2012 = [
    "Jan 26, 2012",
    "Feb 20, 2012",
    "Mar 08, 2012",
    "Apr 05, 2012",
    "Apr 06, 2012",
    "May 01, 2012",
    "Aug 15, 2012",
    "Aug 20, 2012",
    "Sep 19, 2012",
    "Oct 02, 2012",
    "Oct 24, 2012",
    "Oct 26, 2012",
    "Nov 13, 2012",
    "Nov 14, 2012",
    "Nov 28, 2012",
    "Dec 25, 2012",
]


# Dates for 2013
dates_2013 = [
    "Jan 26, 2013",
    "Mar 27, 2013",
    "Mar 29, 2013",
    "Apr 19, 2013",
    "Apr 24, 2013",
    "May 1, 2013",
    "Aug 9, 2013",
    "Aug 15, 2013",
    "Sep 9, 2013",
    "Oct 2, 2013",
    "Oct 16, 2013",
    "Nov 4, 2013",
    "Nov 14, 2013",
    "Nov 15, 2013",
    "Dec 25, 2013",
]

# Dates for 2014
dates_2014 = [
    "Feb 27, 2014",
    "Mar 17, 2014",
    "Apr 8, 2014",
    "Apr 14, 2014",
    "Apr 18, 2014",
    "Apr 24, 2014",
    "May 1, 2014",
    "Jul 29, 2014",
    "Aug 15, 2014",
    "Aug 29, 2014",
    "Oct 2, 2014",
    "Oct 3, 2014",
    "Oct 6, 2014",
    "Oct 15, 2014",
    "Oct 23, 2014",
    "Oct 24, 2014",
    "Nov 4, 2014",
    "Nov 6, 2014",
    "Dec 25, 2014",
]

# Dates for 2015
dates_2015 = [
    "Jan 26, 2015",
    "Feb 17, 2015",
    "Mar 06, 2015",
    "Apr 02, 2015",
    "Apr 03, 2015",
    "Apr 14, 2015",
    "May 01, 2015",
    "Sep 17, 2015",
    "Sep 25, 2015",
    "Oct 02, 2015",
    "Oct 22, 2015",
    "Nov 11, 2015",
    "Nov 12, 2015",
    "Nov 25, 2015",
    "Dec 25, 2015",
]

# Dates for 2016
dates_2016 = [
    "Jan 26, 2016",
    "Mar 07, 2016",
    "Mar 24, 2016",
    "Mar 25, 2016",
    "Apr 14, 2016",
    "Apr 15, 2016",
    "Apr 19, 2016",
    "Jul 06, 2016",
    "Aug 15, 2016",
    "Sep 05, 2016",
    "Sep 13, 2016",
    "Oct 11, 2016",
    "Oct 12, 2016",
    "Oct 31, 2016",

    "Nov 14, 2016",
]
# Dates for 2017
dates_2017 = [
    "Jan 26, 2017",
    "Feb 24, 2017",
    "Mar 13, 2017",
    "Apr 04, 2017",
    "Apr 14, 2017",
    "May 01, 2017",
    "Jun 26, 2017",
    "Aug 15, 2017",
    "Aug 25, 2017",
    "Oct 02, 2017",
    "Oct 19, 2017",
    "Oct 20, 2017",
    "Dec 25, 2017",
]

# Dates for 2018
dates_2018 = [
    "Jan 26, 2018",
    "Feb 13, 2018",
    "Mar 02, 2018",
    "Mar 29, 2018",
    "Mar 30, 2018",
    "May 01, 2018",
    "Aug 15, 2018",
    "Aug 22, 2018",
    "Sep 13, 2018",
    "Sep 20, 2018",
    "Oct 02, 2018",
    "Oct 18, 2018",
    "Nov 07, 2018",
    "Nov 08, 2018",
    "Nov 23, 2018",
    "Dec 25, 2018",
]

# Dates for 2019
dates_2019 = [
    "Mar 04, 2019",
    "Mar 21, 2019",
    "Apr 17, 2019",
    "Apr 19, 2019",
    "Apr 29, 2019",
    "May 01, 2019",
    "Jun 05, 2019",
    "Aug 12, 2019",
    "Aug 15, 2019",
    "Sep 02, 2019",
    "Sep 10, 2019",
    "Oct 02, 2019",
    "Oct 08, 2019",
    "Oct 21, 2019",
    "Oct 28, 2019",
    "Nov 12, 2019",
    "Dec 25, 2019",
]

dates_2020 = [
    "Feb 21, 2020",
    "Mar 10, 2020",
    "Apr 02, 2020",
    "Apr 06, 2020",
    "Apr 10, 2020",
    "Apr 14, 2020",
    "May 01, 2020",
    "May 25, 2020",
    "Oct 02, 2020",
    "Nov 16, 2020",
    "Nov 30, 2020",
    "Dec 25, 2020",
]

# Dates for 2021
dates_2021 = [
    "Jan 26, 2021",
    "Mar 11, 2021",
    "Mar 29, 2021",
    "Apr 02, 2021",
    "Apr 14, 2021",
    "Apr 21, 2021",
    "May 13, 2021",
    "Jul 21, 2021",
    "Aug 19, 2021",
    "Sep 10, 2021",
    "Oct 15, 2021",
    "Nov 04, 2021",
    "Nov 05, 2021",
    "Nov 19, 2021",
]

dates_2022 = [
    "Jan 26, 2022",
    "Mar 01, 2022",
    "Mar 18, 2022",
    "Apr 14, 2022",
    "Apr 15, 2022",
    "May 03, 2022",
    "Aug 09, 2022",
    "Aug 15, 2022",
    "Aug 31, 2022",
    "Oct 05, 2022",
    "Oct 24, 2022",
    "Oct 26, 2022",
    "Nov 08, 2022",
]


# Convert strings to datetime objects
datetime_format = "%b %d, %Y"
dates_2012 = [datetime.strptime(date, datetime_format) for date in dates_2012]
dates_2013 = [datetime.strptime(date, datetime_format) for date in dates_2013]
dates_2014 = [datetime.strptime(date, datetime_format) for date in dates_2014]
dates_2015 = [datetime.strptime(date, datetime_format) for date in dates_2015]
dates_2016 = [datetime.strptime(date, datetime_format) for date in dates_2016]
dates_2017 = [datetime.strptime(date, datetime_format) for date in dates_2017]
dates_2018 = [datetime.strptime(date, datetime_format) for date in dates_2018]
dates_2019 = [datetime.strptime(date, datetime_format) for date in dates_2019]
dates_2020 = [datetime.strptime(date, datetime_format) for date in dates_2020]
dates_2021 = [datetime.strptime(date, datetime_format) for date in dates_2021]
dates_2022 = [datetime.strptime(date, datetime_format) for date in dates_2022]
# Combine the arrays
holidays_in_holding = dates_2013 + dates_2014 + dates_2015 + dates_2016 + dates_2017 + dates_2018 + dates_2019 + dates_2020 + dates_2021 + dates_2022
holidays_in_lookback = dates_2012 + dates_2013 + dates_2014 + dates_2015 + dates_2016 + dates_2017 + dates_2018 + dates_2019 + dates_2020 + dates_2021 + dates_2022

trading_weeks_beginnings = []

for monday in mondays_array:
    if not monday in holidays_in_holding:
        trading_weeks_beginnings.append(monday)
        continue

    tuesday = monday + timedelta(days=1)
    if tuesday not in holidays_in_holding:
        trading_weeks_beginnings.append(tuesday)
        continue

    wednesday = monday + timedelta(days=2)
    if wednesday not in holidays_in_holding:
        trading_weeks_beginnings.append(wednesday)
        continue

    thursday = monday + timedelta(days=3)
    if thursday not in holidays_in_holding:
        trading_weeks_beginnings.append(thursday)
        continue

    friday = monday + timedelta(days=4)
    if friday not in holidays_in_holding:
        trading_weeks_beginnings.append(friday)
        continue

lookback_dates_list = []

for monday in many_mondays:
    if not monday in holidays_in_lookback:
        lookback_dates_list.append(monday)
        continue

    tuesday = monday + timedelta(days=1)
    if tuesday not in holidays_in_lookback:
        lookback_dates_list.append(tuesday)
        continue

    wednesday = monday + timedelta(days=2)
    if wednesday not in holidays_in_lookback:
        lookback_dates_list.append(wednesday)
        continue

    thursday = monday + timedelta(days=3)
    if thursday not in holidays_in_lookback:
        lookback_dates_list.append(thursday)
        continue

    friday = monday + timedelta(days=4)
    if friday not in holidays_in_lookback:
        lookback_dates_list.append(friday)
        continue

strategy_trading_dates = np.array([date.strftime('%Y-%m-%d') for date in trading_weeks_beginnings])
lookback_dates_list = np.array([date.strftime('%Y-%m-%d') for date in lookback_dates_list])
# answer = pd.read_csv('Sample NAV.csv')

#endregion


#region     All get_any_date functions 
def get_lookback_date(input_date, num_weeks, lookback_dates_list):
    try:
        date_index = np.where(np.array(lookback_dates_list) == input_date)[0][0]
    except IndexError:
        print(f"Error: {input_date} not found in lookback_dates_list.")
        return None

    new_index = date_index - num_weeks

    new_index = max(new_index, 0)

    new_date = lookback_dates_list[new_index]
    return new_date

def get_sell_date(input_date, num_weeks, many_dates_list):
    try:
        date_index = np.where(np.array(many_dates_list) == input_date)[0][0]
        # print("Got it!")
    except IndexError:
        print(f"Error: {input_date} not found in many_dates_list.")
        return None

    new_index = date_index + num_weeks

    new_index = max(new_index, 0)

    new_date = many_dates_list[new_index]
    return new_date

def is_weekday(dt):
    return dt.weekday() < 5  # Monday to Friday are considered weekdays

def find_previous_weekday_not_holiday(input_date, holidays):
    current_date = input_date
    current_date = pd.to_datetime(current_date)
    while True:
        # Find the date just prior to the current date
        current_date -= timedelta(days=1)

        # Check if it is a weekday and not a holiday
        if is_weekday(current_date) and current_date not in holidays:
            return current_date
#endregion


#region     All functions to calculate percentage_differences,get missing data from yfinance
def calculate_percentage_returns(top_N_companies, buy_date, sell_date):
    # Convert date columns to datetime objects
    all_data['NDP_Date'] = pd.to_datetime(all_data['NDP_Date'], format='%d-%b-%Y')

    # Extracting codes from top_N_companies dataframe
    codes = top_N_companies['CD_Bloomberg Code'].tolist()

    # Filtering all_data dataframe based on codes and buy_date/sell_date
    filtered_all_data_buy = all_data[(all_data['CD_Bloomberg Code'].isin(codes)) & (all_data['NDP_Date'] == pd.to_datetime(buy_date, format='%d-%b-%Y'))]
    filtered_all_data_sell = all_data[(all_data['CD_Bloomberg Code'].isin(codes)) & (all_data['NDP_Date'] == pd.to_datetime(sell_date, format='%d-%b-%Y'))]

    # Drop duplicates from buy and sell dataframes
    filtered_all_data_sell = filtered_all_data_sell.drop_duplicates(subset=['CD_Bloomberg Code'])
    filtered_all_data_buy = filtered_all_data_buy.drop_duplicates(subset=['CD_Bloomberg Code'])

    # Merging buy and sell dataframes on 'CD_Bloomberg Code'
    merged_data = pd.merge(filtered_all_data_buy, filtered_all_data_sell, on='CD_Bloomberg Code', suffixes=('_buy', '_sell'))

    # Initialize a dictionary to store Bloomberg Codes and their percentage returns
    percentage_returns_dict = {}

    # Calculate percentage returns and store in the dictionary
    for _, row in merged_data.iterrows():
        sell_price = pd.to_numeric(row['NDP_Close_sell'])
        buy_price = pd.to_numeric(row['NDP_Close_buy'])
        percentage_return = ((sell_price - buy_price) / buy_price) * 100
        code = row['CD_Bloomberg Code']
        percentage_returns_dict[code] = percentage_return

    # Create a list of percentage returns based on the original order of Bloomberg Codes
    percentage_returns_list = [percentage_returns_dict.get(code, math.nan) for code in codes]
    
    return percentage_returns_list


def get_diff(company_name, lookback_date, buy_date):
    ticker = find_symbol(company_name)
    if ticker is None:
        return float('nan')
    
    try:
        with warnings.catch_warnings(), contextlib.redirect_stderr(io.StringIO()):
            warnings.simplefilter("ignore")
            stock_data = yf.download(ticker, start=lookback_date, end=buy_date, progress=False)

        if stock_data.empty:
            return float('nan')

        closing_prices = stock_data['Close']
        price_diff_pct = ((closing_prices.iloc[-1] - closing_prices.iloc[0]) / closing_prices.iloc[0]) * 100

        return price_diff_pct

    except Exception:
        return float('nan')

symbol_cache = {}

def find_symbol(company_name, retries=5, base_delay=2):
    company_name = company_name.replace(" Ltd.", "").strip()

    if company_name in symbol_cache:
        return symbol_cache[company_name]

    for attempt in range(retries):
        try:
            search_result = yq_search(company_name)

            if 'quotes' in search_result and search_result['quotes']:
                symbol = search_result['quotes'][0]['symbol']
                symbol_cache[company_name] = symbol
                return symbol
            else:
                # print(Fore.YELLOW + f"No results found for '{company_name}'" + Style.RESET_ALL)
                return None

        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                # print(Fore.RED + f"Rate limited on '{company_name}'. Retrying in {delay:.2f}s..." + Style.RESET_ALL)
                if delay>5:
                    return None
                time.sleep(delay)
            else:
                # print(Fore.RED + f"Error searching for '{company_name}': {e}" + Style.RESET_ALL)
                return None

    # print(Fore.RED + f"Failed to retrieve symbol for '{company_name}' after {retries} attempts." + Style.RESET_ALL)
    return None
#endregion


#region     NSE500 check
constituents_path = r"/Users/priyaanksheth/Downloads/Momentum/NSE500_Components"
c1 = pd.read_csv(os.path.join(constituents_path, 'apr2014.csv'))
c2 = pd.read_csv(os.path.join(constituents_path, 'apr2015.csv'))
c3 = pd.read_csv(os.path.join(constituents_path, 'apr2016.csv'))
c4 = pd.read_csv(os.path.join(constituents_path, 'apr2017.csv'))
c5 = pd.read_csv(os.path.join(constituents_path, 'apr2018.csv'))
c6 = pd.read_csv(os.path.join(constituents_path, 'apr2019.csv'))
c7 = pd.read_csv(os.path.join(constituents_path, 'apr2020.csv'))
c8 = pd.read_csv(os.path.join(constituents_path, 'apr2021.csv'))
c9 = pd.read_csv(os.path.join(constituents_path, 'apr2022.csv'))
d1 = pd.read_csv(os.path.join(constituents_path, 'oct2013.csv'))
d2 = pd.read_csv(os.path.join(constituents_path, 'oct2014.csv'))
d3 = pd.read_csv(os.path.join(constituents_path, 'oct2015.csv'))
d4 = pd.read_csv(os.path.join(constituents_path, 'oct2016.csv'))
d5 = pd.read_csv(os.path.join(constituents_path, 'oct2017.csv'))
d6 = pd.read_csv(os.path.join(constituents_path, 'oct2018.csv'))
d7 = pd.read_csv(os.path.join(constituents_path, 'oct2019.csv'))
d8 = pd.read_csv(os.path.join(constituents_path, 'oct2020.csv'))
d9 = pd.read_csv(os.path.join(constituents_path, 'oct2021.csv'))

dfs = [globals()['d' + str(i)] for i in range(1, 10)]

for i, df in enumerate(dfs, start=1):
    df.columns = df.iloc[0]
    
    # Drop the first row
    df = df.drop(0)
    
    # Drop rows with missing values
    df = df.dropna()
    
    # Update the original DataFrame in the list
    globals()['d' + str(i)] = df

dfs = [globals()['c' + str(i)] for i in range(1, 9)]

for i, df in enumerate(dfs, start=1):
    df.columns = df.iloc[0]
    
    # Drop the first row
    df = df.drop(0)
    
    # Drop rows with missing values
    df = df.dropna()
    
    # Update the original DataFrame in the list
    globals()['c' + str(i)] = df

def in_nse500(company_array,date):
    indices_not_found = []
    i_final = None
    date_ranges = [
        #("01-04-2013", "30-09-2013"),
        ("01-10-2013", "31-03-2014"),
        ("01-04-2014", "30-09-2014"),
        ("01-10-2014", "31-03-2015"),
        ("01-04-2015", "30-09-2015"),
        ("01-10-2015", "31-03-2016"),
        ("01-04-2016", "30-09-2016"),
        ("01-10-2016", "31-03-2017"),
        ("01-04-2017", "30-09-2017"),
        ("01-10-2017", "31-03-2018"),
        ("01-04-2018", "30-09-2018"),
        ("01-10-2018", "31-03-2019"),
        ("01-04-2019", "30-09-2019"),
        ("01-10-2019", "31-03-2020"),
        ("01-04-2020", "30-09-2020"),
        ("01-10-2020", "31-03-2021"),
        ("01-04-2021", "30-09-2021"),
        ("01-10-2021", "31-03-2022"),
        ("01-04-2022", "30-09-2022")
    ]
    search_list = [d1, c1, d2, c2, d3, c3, d4, c4, d5, c5, d6, c6, d7, c7, d8, c8, d9, c9]
    for i, (start, end) in enumerate(date_ranges):
        lookback_date = datetime.strptime(start, "%d-%m-%Y")
        buy_date = datetime.strptime(end, "%d-%m-%Y")
        if lookback_date <= date <= buy_date:
            i_final = i
            
    if i_final == None:                 # Didn't find the date to be in our time universe
        return True

    for index, company in enumerate(company_array):
        if not any(entry == company for entry in search_list[i_final]["Security Name"]):
            #print("NSE Components Issue on ",date)
            indices_not_found.append(index)
        else:
            continue
    if indices_not_found:
        return indices_not_found
    return True
#endregion


#region     Miscallenous
row = 1
setting_value_count = 0         # Number of Companies we set values RF = 100

#endregion

#region     Loop Function
def process_date(index, buy_date):
    # print("NAV = ", """Fore.BLUE + str(np.round(NAV))""", " at date = ", Fore.GREEN + buy_date)
    
    if index >= (len(strategy_trading_dates) - 2):
        return None
    week_date = {1:None,2:None,3:None,4:None}

    lookback_date = get_lookback_date(buy_date, 48, lookback_dates_list)
    sell_date = get_sell_date(buy_date, 1, lookback_dates_list)
    # dates_for_NAV_calculation = [buy_date]
    if holding=="monthly":
        end_at = 5
    else:
        end_at = 2
    for i in range(1,end_at):
        week_date[i] = get_sell_date(buy_date,i,lookback_dates_list)
        # dates_for_NAV_calculation.append(week_date[i])
        week_date[i] = pd.to_datetime(week_date[i])
    lookback_date = pd.to_datetime(lookback_date)
    look_upto_date = find_previous_weekday_not_holiday(buy_date, holidays_in_lookback)
    buy_date = pd.to_datetime(buy_date)
    sell_date = pd.to_datetime(sell_date)
    rf_dict = {1:[],2:[],3:[],4:[]}
    
    filtered_df = filtered_components[(filtered_components['NDP_Date'] >= lookback_date) & (filtered_components['NDP_Date'] <= buy_date)]
    filtered_df['NDP_Close'] = pd.to_numeric(filtered_df['NDP_Close'])
    filtered_df['NDP_Date'] = pd.to_datetime(filtered_df['NDP_Date'])

    data_on_lookback_date = filtered_df.loc[filtered_df['NDP_Date'] == lookback_date, ['CD_Bloomberg Code', 'NDP_Close']]
    data_on_lookupto_date = filtered_df.loc[filtered_df['NDP_Date'] == look_upto_date, ['CD_Bloomberg Code', 'NDP_Close']]
    
    result_df = pd.merge(data_on_lookback_date, data_on_lookupto_date, on='CD_Bloomberg Code', suffixes=('_lookback', '_lookupto'))
    result_df['Percentage Change'] = ((result_df['NDP_Close_lookupto'] - result_df['NDP_Close_lookback']) / result_df['NDP_Close_lookback']) * 100
    
    result_df = result_df.merge(filtered_df[['CD_Bloomberg Code', 'Company Name']], on='CD_Bloomberg Code')
    result_df = result_df.drop_duplicates(subset='CD_Bloomberg Code')
    top_N_companies = result_df.nlargest(60, 'Percentage Change')
    top_N_companies.reset_index(drop=True, inplace=True)
    
    company_array = top_N_companies['Company Name'].values
    check_nse_components = in_nse500(company_array, buy_date)
    if check_nse_components is True:
        top_N_companies = top_N_companies.head(N)
    else:
        indicess_not_found = check_nse_components
        top_N_companies = top_N_companies.drop(indicess_not_found)
        top_N_companies = top_N_companies.head(N)

    if len(top_N_companies) != N:
        print(f"We are going forward with less than {N} companies, {len(top_N_companies)}")
        return None

    ticker_array = top_N_companies['CD_Bloomberg Code'].values
    if len(ticker_array) == 0:
        # print("continuing because of len(ticker_array)")
        return None
    sum_momentum = top_N_companies['Percentage Change'].sum()
    
    # rf_array = calculate_percentage_returns(top_N_companies, buy_date, sell_date)
    check_date_list = [buy_date]
    for i in range(1,end_at):
        rf_dict[i] = calculate_percentage_returns(top_N_companies, check_date_list[-1],week_date[i])
        # print(f"calculating returns from {check_date_list[-1]} to {week_date[i]}")
        check_date_list.append(week_date[i])
        rf_dict[i] = [x + 100 for x in rf_dict[i]]
        # print(rf_dict[i])
    # top_N_companies["Returns"] = rf_array
    
        nan_indices = np.where(np.isnan(rf_dict[i]))[0]
        for indice in nan_indices:
            company_name = company_array[indice]
            lookback_date = buy_date
            buy_date = sell_date
            value = get_diff(company_name, lookback_date, buy_date)
        
        nan_indices = np.where(np.isnan(rf_dict[i]))[0]
        for indice in nan_indices:
            # setting_value_count += 1
            # print("setting ", setting_value_count)
            rf_dict[i][indice] = 100
        for rf in rf_dict[i]:
            if rf>200:
                print("RF ======= ", rf )
    with print_lock:
        profit = np.sum(rf_dict[i]) - 5000
        profit_color = Fore.GREEN if profit >= 0 else Fore.RED

        print(
            Fore.BLUE + str(buy_date),
            "Profit =",
            profit_color + str(round(profit, 2)) + Style.RESET_ALL + "\n"
        )
    check_date_list = [buy_date]

    new_rows = {}
    for i in range(1,end_at):
        new_rows[i] = {
        'Date': check_date_list[-1],
        'Momentum': sum_momentum,
        }
        check_date_list.append(week_date[i])

        new_rows[i].update({f'Ticker {j}': ticker_array[j-1] for j in range(1, 51)})
        new_rows[i].update({f'Name {j}': company_array[j-1] for j in range(1, 51)})
        new_rows[i].update({f'Rf{j}': rf_dict[i][j-1] for j in range(1, 51)})
    return new_rows
#endregion

start = time.perf_counter()
# Main Loop
N = 50                          # Number of Companies we want to buy each time
holding = "monthly"             # Weekly or Monthly
strategy_trading_dates = strategy_trading_dates[:-12]
if holding =="monthly":
    strategy_trading_dates = strategy_trading_dates[0::4]
with ThreadPoolExecutor(max_workers=5) as executor:
    future_to_date = {executor.submit(process_date, index, buy_date): buy_date for index, buy_date in enumerate(strategy_trading_dates)}
    for future in as_completed(future_to_date):
        new_rows = future.result()
        if new_rows is not None:
            if isinstance(new_rows, dict):
                new_rows = pd.DataFrame(new_rows).T
            answer = pd.concat([answer, new_rows], ignore_index=True)

answer.sort_values(by='Date', inplace=True)
answer.reset_index(drop=True, inplace=True)

NAV = 1000
for index, row in answer.iterrows():
    answer.iloc[index,1] = NAV
    rf_array = row[103:153].tolist()
    rf_array = np.array(rf_array, dtype=float)
    components = (NAV/N)*rf_array/100
    NAV = np.sum(components)
    answer.iloc[index, 153:203] = components
    
#region      Adding benchmark NAV
backtest_cache_path = r"/Users/priyaanksheth/Downloads/Momentum/Backtest_cache"
output_path  = os.path.join(backtest_cache_path,"answer_check.csv")
nse500_path = r"/Users/priyaanksheth/Downloads/Momentum/NSE500_Components/index_data.csv"

# Load data
nse500 = pd.read_csv(nse500_path)

# Convert date columns
answer["Date"] = pd.to_datetime(answer["Date"])
nse500["Date"] = pd.to_datetime(nse500["Date"], format="%d/%m/%y")

# Reduce to relevant columns
nse500 = nse500[["Date", "Close"]].sort_values("Date")

# Find start NAV index
initial_backtest_date = answer["Date"].min()
initial_benchmark_row = nse500[nse500["Date"] <= initial_backtest_date].iloc[-1]
initial_index_value = initial_benchmark_row["Close"]

# Merge benchmark index using nearest earlier date
answer_sorted = answer.sort_values("Date")
merged = pd.merge_asof(answer_sorted, nse500, on="Date", direction="backward")

# Calculate NAV_benchmark
merged["NAV_benchmark"] = (merged["Close"] / initial_index_value) * 1000

cols = list(merged.columns)

if "NAV" in cols and "NAV_benchmark" in cols:
    nav_index = cols.index("NAV")
    # Remove NAV_benchmark and re-insert right after NAV
    cols.remove("NAV_benchmark")
    cols.insert(nav_index + 1, "NAV_benchmark")
    merged = merged[cols]
#endregion

# Save to CSV
merged.to_csv(output_path, index=False)
print(f"\n✅ Saved to {output_path}")

if os.name == 'posix':  # Command for macOS to open sheet
    subprocess.run(['open', output_path], check=True) 


finish = time.perf_counter()
print("Backested completed in ", finish - start ,"s")
