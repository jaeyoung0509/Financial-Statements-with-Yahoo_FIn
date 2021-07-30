import yahoo_fin.stock_info as si 
from multiprocessing import Pool
import pandas as pd

#get each stock's info
def get_stockinfo(stock_ticker):
    try:
        print(stock_ticker)
        stock_info = si.get_quote_table(stock_ticker, dict_result=True)
        #for change '.' -> '-'  , 1. divide key list,val list
        stock_val = list(stock_info.values())
        stock_key = list(stock_info.keys())
        #2.confrim whether there are dot('.') and replace('-')
        for idx,val in enumerate(stock_key) :
            if  "." in val:
                stock_key[idx] = val.replace("." , "-")
        #3.and put back together
        store_stockInfo = dict()
        store_stockInfo['ticker'] = stock_ticker
        for x, y in zip(stock_key , stock_val):
            store_stockInfo[x] = y #x is key, y is val
        return store_stockInfo
    except Exception as e:
        return None

def writeExcel(df, name):
    writer = pd.ExcelWriter(name+'.xlsx', engine='xlsxwriter')
    df.to_excel(writer, sheet_name='welcome', index=False)
    writer.save()

def change_marketGap(df):
    df = df.astype({'Market Cap' : 'str'})
    #1 -> K(1,000) -> M(1,000,000) -> B(1,000,000,000) -> T(1,000,000,000,000)
    df["Market Cap"] = df["Market Cap"].apply(lambda x: float(x[:-1]) * (10 **3) if x[-1] == 'K'
    else float(x[:-1]) * (10 **6) if  x[-1] == 'M'
    else float(x[:-1]) * (10 **9) if  x[-1] == 'B'
    else float(x[:-1]) * (10 **12) if  x[-1] == 'T'
    else x)
    df = df.astype({'Market Cap' : 'float'})
    df.sort_values(by = ['Market Cap'] , axis = 0 , ascending = False , inplace = True)
    print(df[['ticker' , 'Market Cap']])
    return df

if __name__ == '__main__':
    #get Ticker
    nasdaq_tickers = si.tickers_nasdaq()
    sp500_tickers = si.tickers_sp500()
    #multi-processing 
    pool = Pool(30)
    sp500_info =  list(filter (None , pool.map(get_stockinfo , sp500_tickers)))
    nasdaq_info = list(filter (None , pool.map(get_stockinfo ,nasdaq_tickers)))
    #change Market Cap str -> float
    nasdaq_info = change_marketGap(nasdaq_info)
    sp500_info = change_marketGap(sp500_info)
    writeExcel(nasdaq_info, 'nasdaq')
    writeExcel(sp500_info, 'sp500')
    