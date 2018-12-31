# -*- coding: utf-8 -*-
"""
Created on Mon Dec 24 13:44:51 2018

@author: Jeff
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from datetime import datetime
import os
import requests
import json
import matplotlib.pyplot as plt
import pandas as pd
from collections import OrderedDict
import math
import urllib3

def matching(string, begTok, endTok):
    # Find location of the beginning token
    start = string.find(begTok)
    stack = []
    # Append it to the stack
    stack.append(start)
    # Loop through rest of the string until we find the matching ending token
    for i in range(start+1, len(string)):
        if begTok in string[i]:
            stack.append(i)
        elif endTok in string[i]:
            stack.remove(stack[-1])
        if len(stack) == 0:
            # Removed the last begTok so we're done
            end = i+1
            break
    return end

def chunkIt(seq, num):
    # Splits a range in num roughly equal parts
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out

def parseSummary(ticker):
    # Yahoo Finance summary for stock, mutual fund or ETF
    url = "http://finance.yahoo.com/quote/%s?p=%s"%(ticker,ticker)
    response = requests.get(url, verify=False)
    print ("Parsing %s"%(url))
    time.sleep(4)
    summary_data = OrderedDict()

    # Convert the _context html object into JSON blob to tell if this is an equity, a mutual fund or an ETF
    contextStart = response.text.find('"_context"')
    contextEnd = contextStart+matching(response.text[contextStart:len(response.text)], '{', '}')
    
    # Convert the QuoteSummaryStore html object into JSON blob
    summaryStart = response.text.find('"QuoteSummaryStore"')
    summaryEnd = summaryStart+matching(response.text[summaryStart:len(response.text)], '{', '}')
    
    # Convert the ticker quote html object into JSON blob
    streamStart = response.text.find('"StreamDataStore"')
    quoteStart = streamStart+response.text[streamStart:len(response.text)].find("%s"%ticker.upper())-1
    quoteEnd = quoteStart+matching(response.text[quoteStart:len(response.text)], '{', '}')
    
    # Decode the JSON blobs
    json_loaded_context = json.loads('{' + response.text[contextStart:contextEnd] + '}')
    json_loaded_summary = json.loads('{' + response.text[summaryStart:summaryEnd] + '}')
    # Didn't end up needing this for the summary details, but there's lots of good data there
    json_loaded_quote = json.loads('{' + response.text[quoteStart:quoteEnd] + '}')
    if "MUTUALFUND" in json_loaded_context["_context"]["quoteType"] or "ETF" in json_loaded_context["_context"]["quoteType"]:
        # Define all the data that appears on the Yahoo Financial summary page for a mutual fund
        # Use http://beautifytools.com/html-beautifier.php to understand where the path came from or to add any additional data
        returns = json_loaded_summary["QuoteSummaryStore"]["fundPerformance"]["trailingReturns"]
        summary_data.update({"1-Month":returns["oneMonth"]["fmt"]})
        summary_data.update({"3-Month":returns["threeMonth"]["fmt"]})
        summary_data.update({"YTD":returns["ytd"]["fmt"]})
        summary_data.update({"1-Year":returns["oneYear"]["fmt"]})
        summary_data.update({"3-Year":returns["threeYear"]["fmt"]})
        summary_data.update({"5-Year":returns["fiveYear"]["fmt"]})
        summary_data.update({"10-Year":returns["tenYear"]["fmt"]})
        summary_data.update({"Last Bull Market":returns["lastBullMkt"]["fmt"]})
        summary_data.update({"Last Bear Market":returns["lastBearMkt"]["fmt"]})
        
        holdings = json_loaded_summary["QuoteSummaryStore"]["topHoldings"]["equityHoldings"]
        summary_data.update({"Price/Earnings":holdings["priceToEarnings"]["fmt"]})
        summary_data.update({"Price/Book":holdings["priceToBook"]["fmt"]})
        summary_data.update({"Price/Sales":holdings["priceToSales"]["fmt"]})
        summary_data.update({"Price/Cashflow":holdings["priceToCashflow"]["fmt"]})
    
        return summary_data

def parseHistorical(ticker):
    # Yahoo Finance historical data for stock, mutual fund or ETF
    url = "https://finance.yahoo.com/quote/%s/history"%(ticker)
    response = requests.get(url, verify=False)
    print ("Parsing %s"%(url))
    time.sleep(4)

    # Convert the _context html object into JSON blob to tell if this is an equity, a mutual fund or an ETF
    contextStart = response.text.find('"_context"')
    contextEnd = contextStart+matching(response.text[contextStart:len(response.text)], '{', '}')
    
    # Convert the HistoricalPriceStore html object into JSON blob
    pricesStart = response.text.find('"HistoricalPriceStore"')
    pricesEnd = pricesStart+matching(response.text[pricesStart:len(response.text)], '{', '}')
    
    # Decode the JSON blobs
    json_loaded_context = json.loads('{' + response.text[contextStart:contextEnd] + '}')
    json_loaded_prices = json.loads('{' + response.text[pricesStart:pricesEnd] + '}')
    
    if "EQUITY" in json_loaded_context["_context"]["quoteType"] or "MUTUALFUND" in json_loaded_context["_context"]["quoteType"] or "ETF" in json_loaded_context["_context"]["quoteType"]:
        # Use http://beautifytools.com/html-beautifier.php to understand where the path came from or to add any additional data
        first_trade_date = json_loaded_prices["HistoricalPriceStore"]["firstTradeDate"]
        
        # Yahoo Finance historical data for mutual fund or ETF
        # from first trade date to today
        # int(time.mktime(datetime.strptime("12/25/2018", "%m/%d/%Y").timetuple()))
        url = "https://finance.yahoo.com/quote/%s/history?period1=%d&period2=%d&interval=1d&filter=history&frequency=1d"%(ticker,first_trade_date,int(time.time()))
        response = requests.get(url, verify=False)
        print ("Parsing %s"%(url))
        time.sleep(4)
        
        # Convert the HistoricalPriceStore html object into JSON blob
        pricesStart = response.text.find('"HistoricalPriceStore"')
        pricesEnd = pricesStart+matching(response.text[pricesStart:len(response.text)], '{', '}')
        
        # Decode the JSON blob
        json_loaded_prices = json.loads('{' + response.text[pricesStart:pricesEnd] + '}')
        days = json_loaded_prices["HistoricalPriceStore"]["prices"]
        
        # Remove the reporting of dividends
        days[:] = [d for d in days if 'adjclose' in d]
        
        # Remove unneeded dictionary keys
        delKeys = ["open","high","low","close","volume"]
        for day in days:
            for delkey in delKeys:
                del day[delkey]
            
            # Change date from unixtimestamp to human readable date
            day["date"] = datetime.utcfromtimestamp(day["date"])#.strftime('%Y%m%d')
        
        # Sort in ascending date order
        days = sorted(days, key=lambda d: d['date'])
    
        # Determine varius moving averages
        ma20 = pd.DataFrame([d['adjclose'] for d in days]).rolling(window=20).mean().iloc[20-1:].replace(math.nan, '', regex=True)
        ma50 = pd.DataFrame([d['adjclose'] for d in days]).rolling(window=50).mean().iloc[50-1:].replace(math.nan, '', regex=True)
        ma100 = pd.DataFrame([d['adjclose'] for d in days]).rolling(window=100).mean().iloc[100-1:].replace(math.nan, '', regex=True)
        ma150 = pd.DataFrame([d['adjclose'] for d in days]).rolling(window=150).mean().iloc[150-1:].replace(math.nan, '', regex=True)
        ma200 = pd.DataFrame([d['adjclose'] for d in days]).rolling(window=200).mean().iloc[200-1:].replace(math.nan, '', regex=True)
        
        # Ensure moving averages are same length as the number of days
        ma20 = list(pd.concat([pd.DataFrame(['' for i in range(len(days)-len(ma20))]),ma20], axis=0).values.flatten())
        ma50 = list(pd.concat([pd.DataFrame(['' for i in range(len(days)-len(ma50))]),ma50], axis=0).values.flatten())
        ma100 = list(pd.concat([pd.DataFrame(['' for i in range(len(days)-len(ma100))]),ma100], axis=0).values.flatten())
        ma150 = list(pd.concat([pd.DataFrame(['' for i in range(len(days)-len(ma150))]),ma150], axis=0).values.flatten())
        ma200 = list(pd.concat([pd.DataFrame(['' for i in range(len(days)-len(ma200))]),ma200], axis=0).values.flatten())
        
        # Add the moving averages to the days list
        for i in range(len(days)):
            days[i]['ma20'] = ma20[i]
            days[i]['ma50'] = ma50[i]
            days[i]['ma100'] = ma100[i]
            days[i]['ma150'] = ma150[i]
            days[i]['ma200'] = ma200[i]
    
        # Need to find adjclose from last day of last year for year to date calculation
        df = pd.DataFrame({'date':[d['date'] for d in days], 'adjclose':[d['adjclose'] for d in days]}).groupby(pd.Grouper(key='date',freq='A'))
        ytd = (days[-1]['adjclose']-days[list(df.groups.values())[-2]-1]['adjclose'])/days[list(df.groups.values())[-2]-1]['adjclose']
        
        # Groupby needed datetimes but we'll write a string to sheets
        for day in days:
            day["date"] = day["date"].strftime('%Y-%m-%d')
                
        # Determine varius duration returns
        daily_return = list(pd.DataFrame([d['adjclose'] for d in days]).pct_change(1).replace(math.nan, '', regex=True).values.flatten()) # 1 for ONE DAY lookback
        weekly_return = list(pd.DataFrame([d['adjclose'] for d in days]).pct_change(5).replace(math.nan, '', regex=True).values.flatten()) # 5 for ONE WEEK lookback
        monthly_return = list(pd.DataFrame([d['adjclose'] for d in days]).pct_change(21).replace(math.nan, '', regex=True).values.flatten()) # 21 for ONE MONTH lookback
        three_month_return = list(pd.DataFrame([d['adjclose'] for d in days]).pct_change(21*3).replace(math.nan, '', regex=True).values.flatten()) # 21 for ONE MONTH lookback
        yearly_return = list(pd.DataFrame([d['adjclose'] for d in days]).pct_change(252).replace(math.nan, '', regex=True).values.flatten()) # 252 for ONE YEAR lookback
        # CAGR calculations for beyond one year
        three_year_return = list(pd.DataFrame([d['adjclose'] for d in days]).pct_change(252*3).add(1).pow(1/3).sub(1).replace(math.nan, '', regex=True).values.flatten()) # 252 for ONE YEAR lookback
        five_year_return = list(pd.DataFrame([d['adjclose'] for d in days]).pct_change(252*5).add(1).pow(1/5).sub(1).replace(math.nan, '', regex=True).values.flatten()) # 252 for ONE YEAR lookback
        ten_year_return = list(pd.DataFrame([d['adjclose'] for d in days]).pct_change(252*10).add(1).pow(1/10).sub(1).replace(math.nan, '', regex=True).values.flatten()) # 252 for ONE YEAR lookback
        fifteen_year_return = list(pd.DataFrame([d['adjclose'] for d in days]).pct_change(252*15).add(1).pow(1/15).sub(1).replace(math.nan, '', regex=True).values.flatten()) # 252 for ONE YEAR lookback
        twenty_year_return = list(pd.DataFrame([d['adjclose'] for d in days]).pct_change(252*20).add(1).pow(1/20).sub(1).replace(math.nan, '', regex=True).values.flatten()) # 252 for ONE YEAR lookback
        
        # Add the returns to the days list
        for i in range(len(days)):
            days[i]['daily_return'] = daily_return[i]
            days[i]['weekly_return'] = weekly_return[i]
            days[i]['monthly_return'] = monthly_return[i]
            days[i]['three_month_return'] = three_month_return[i]
            days[i]['yearly_return'] = yearly_return[i]
            days[i]['three_year_return'] = three_year_return[i]
            days[i]['five_year_return'] = five_year_return[i]
            days[i]['ten_year_return'] = ten_year_return[i]
            days[i]['fifteen_year_return'] = fifteen_year_return[i]
            days[i]['twenty_year_return'] = twenty_year_return[i]
        
        # Put the year to date in the last day
        days[-1]['ytd'] = ytd
        
        # Clean up to release some memory
        del df, ma20, ma50, ma100, ma150, ma200
        del daily_return, weekly_return, monthly_return, three_month_return\
        , yearly_return, three_year_return, five_year_return\
        , ten_year_return, fifteen_year_return, twenty_year_return
        
        return days

if __name__ == "__main__":
    # Python installation per:
    # https://www.scrapehero.com/how-to-install-python3-in-windows-10/
    
    # Based on approach defined:
    # https://www.scrapehero.com/scrape-yahoo-finance-stock-market-data/
    
    # Change directory to the location of the private key
    os.chdir(r'C:\Users\Jeff\Documents\yahoo_finance')
    
    # Disable InsecureRequestWarning: Unverified HTTPS request is being made
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Connection to Google Sheets based on guide:
    # https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html
    # with scope definition as modified:
    # https://stackoverflow.com/questions/49258566/gspread-authentication-throwing-insufficient-permission/49295205#49295205
    
    # use creds to create a client to interact with the Google Drive API
    #scope = ['https://spreadsheets.google.com/feeds']
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)
    
    # Find a workbook by name and open
    # Make sure you use the right name here.
    sheet = client.open("Funds By Asset Class")
    worksheet = sheet.worksheet("Funds By Asset Class")
    
    # Get the ticker symbols
    tickers = worksheet.col_values(2)
    
    # Split up the calls to use all 7 days of the week
    chunks = chunkIt(range(len(tickers)-2), 7)
    
    # Loop through today's chunk of ticker symbols but skip the first 2 header rows
    for ch in chunks[datetime.today().weekday()]:
        x = ch+2 # Ignore header rows
        # Skip blank tickers
        if tickers[x] == '':
            continue
        
        print ("Fetching data for %s"%(tickers[x]))
        tries = 3 # Number of times to try
        for attempt in range(tries):
            try:
                # Get the measures of value from the Holdings page and
                # Performance returns from the Performance page
                summaryData = parseSummary(tickers[x])
                # Get the Historical Data from first trade date to today
                # Then use it to calculate moving averages and returns (Ones from the performance page seem broke in December of 2018)
                historicalData = parseHistorical(tickers[x])
            except KeyError as e:
                if attempt < tries - 1:
                    print ("Retrying...")
                    continue
                else:
                    raise
            break
        
        # Add moving average measures to the performance data
        summaryData.update({"20-dma":historicalData[-1]['ma20']})
        summaryData.update({"50-dma":historicalData[-1]['ma50']})
        summaryData.update({"100-dma":historicalData[-1]['ma100']})
        summaryData.update({"150-dma":historicalData[-1]['ma150']})
        summaryData.update({"200-dma":historicalData[-1]['ma200']})
        # Yahoo Finance currently has incorrect performance data so replace it
        # with calculations from Historical Data
        # In addition add other measures shown at Morningstar
        summaryData.update({"1-Day":historicalData[-1]['daily_return']})
        summaryData.update({"1-Week":historicalData[-1]['weekly_return']})
        summaryData["1-Month"] = historicalData[-1]['monthly_return']
        summaryData["3-Month"] = historicalData[-1]['three_month_return']
        summaryData["YTD"] = historicalData[-1]['ytd']
        summaryData["1-Year"] = historicalData[-1]['yearly_return']
        summaryData["3-Year"] = historicalData[-1]['three_year_return']
        summaryData["5-Year"] = historicalData[-1]['five_year_return']
        summaryData["10-Year"] = historicalData[-1]['ten_year_return']
        summaryData.update({"15-Year":historicalData[-1]['fifteen_year_return']})
        summaryData.update({"20-Year":historicalData[-1]['twenty_year_return']})
        
        # Define the range of cells we'll be updating
        range_build = 'F' + str(x+1) + ':Y' + str(x+1)
        cell_list = worksheet.range(range_build)
        
        # Update the cell_list values with the scraped data
        cell_list[0].value = summaryData['20-dma']
        cell_list[1].value = summaryData['50-dma']
        cell_list[2].value = summaryData['100-dma']
        cell_list[3].value = summaryData['150-dma']
        cell_list[4].value = summaryData['200-dma']
        cell_list[5].value = summaryData['1-Day']
        cell_list[6].value = summaryData['1-Week']
        cell_list[7].value = summaryData['1-Month']
        cell_list[8].value = summaryData['3-Month']
        cell_list[9].value = summaryData['YTD']
        cell_list[10].value = summaryData['1-Year']
        cell_list[11].value = summaryData['3-Year']
        cell_list[12].value = summaryData['5-Year']
        cell_list[13].value = summaryData['10-Year']
        cell_list[14].value = summaryData['15-Year']
        cell_list[15].value = summaryData['20-Year']
        cell_list[16].value = summaryData['Price/Earnings']
        cell_list[17].value = summaryData['Price/Book']
        cell_list[18].value = summaryData['Price/Sales']
        cell_list[19].value = summaryData['Price/Cashflow']
        
        # Write the scraped data to the Google Sheet
        worksheet.update_cells(cell_list)
        
         # See if a worksheet already exists with Historical Data
        wsMissing = True
        for ws in sheet.worksheets():
            if tickers[x] in ws.title:
                wsMissing = False
                histWorksheet = ws
                break
        
        # Create a worksheet for the Historical Data if it wasn't there
        if wsMissing:
            histWorksheet = sheet.add_worksheet(tickers[x], len(historicalData)+10, len(historicalData[0]))
        
        # Define the range of cells we'll be updating
        range_build = 'A1' + ':Q1'
        cell_list = histWorksheet.range(range_build)
        
        # Set the header up to be written
        header = ['Date', 'Adj Close', '20-dma', '50-dma', '100-dma', '150-dma', '200-dma', '1-Day', '1-Week', '1-Month', '3-Month', '1-Year', '3-Year', '5-Year', '10-Year', '15-Year', '20-Year']
        for i, val in enumerate(header):
            cell_list[i].value = val

        # Write the header to the Google Sheet
        histWorksheet.update_cells(cell_list)
        
        # Loop through the elements of historicalData
        for i in range(len(historicalData[0])):
            # Define the range of cells we'll be updating
            range_build =  '{col_i}{row_i}:{col_f}{row_f}'.format(
                    col_i=chr((i) + ord('A')),    # converts number to letter
                    col_f=chr((i) + ord('A')),      # subtract 1 because of 0-indexing
                    row_i=2,
                    row_f=len(historicalData)+1)
            cell_list = histWorksheet.range(range_build)
        
            for j, val in enumerate([d[list(historicalData[0].keys())[i]] for d in historicalData]):
                cell_list[j].value = val
            
            # Write the column to the Google Sheet
            histWorksheet.update_cells(cell_list)
