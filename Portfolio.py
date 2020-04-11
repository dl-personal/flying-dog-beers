import pandas as pd
import numpy as np
import quandl

#TODO: add holings, not get holdings
#TODO; automate finding of ticker, current value columns

class Portfolio:
    def __init__(self,name,quandl_api_key):
        self.name = name

        self.quandl_api_key = quandl_api_key

        self.account_col = 'account'                
        self.ticker_col = 'ticker'
        self.name_col = 'name'
        self.value_col = 'value'
                
        self.holdings = pd.DataFrame(columns=[self.account_col,self.ticker_col,self.name_col,self.value_col])
        self.tickers = pd.Series([])
        
    def add_holdings(self,path,account_name):
        #TODO: add checks for expected input type/schema
        df_holdings = pd.read_csv(path)
        #put in right schema
        
        def detect_keep_cols(df):
            #cols to keep: ticker, name(optional), value
            #TODO: change to add name on at the end from SHARADAR/TICKERS table
            cols = df.columns
        
            possible_ticker_cols = ['Ticker','ticker','Symbol']
            possible_name_cols = ['Description','Name']
            possible_value_cols = ['Current Value']
            
            ticker_col = str(cols[cols.isin(possible_ticker_cols)][0])
            if len(ticker_col) == 0:
                raise Warning('Ticker column not found in holdings account: ' +str(account_name))                
            
            name_col = str(cols[cols.isin(possible_name_cols)][0])
            if len(name_col) == 0:
                raise Warning('Name column not found in holdings file for etf: ' +str(account_name))
                    
            value_col = str(cols[cols.isin(possible_value_cols)][0])
            if len(possible_value_cols) == 0:
                raise Warning('Value column not found in holdings file for etf: ' +str(account_name))            
            
            return [ticker_col,name_col,value_col]
        
        # keep and rename necessary cols               
        keep_cols = detect_keep_cols(df_holdings)        
        df_holdings = df_holdings[keep_cols]        
        df_holdings.columns = [self.ticker_col ,self.name_col,self.value_col]
        df_holdings[self.account_col] = account_name

        # remove $ sign and commas
        df_holdings[self.value_col] = df_holdings[self.value_col].str.replace('$','').str.replace(',','').astype('float')

        # append new holdings to existing holdings
        self.holdings = self.holdings.append(df_holdings)
        
        # reset tickers with new holdings
        self.tickers = pd.Series(self.holdings[self.ticker_col].unique())
        
    # def get_holdings(self,path):
    #     #TODO: add checks for expected input type/schema
    #     df_holdings = pd.read_csv(path)
    #     self.holdings = df_holdings
        
    #     # remove $ sign
    #     curr_val = 'Current Value'
    #     self.holdings[curr_val] = self.holdings[curr_val].str.replace('$','').str.replace(',','').astype('float')
        
    #     # only keep necessary cols
    #     self.ticker_col = 'Symbol'
    #     self.name_col = 'Description'
    #     self.value_col = 'Current Value'
    #     self.holdings_columns = [self.ticker_col ,self.name_col,self.value_col]
    #     self.holdings = self.holdings[self.holdings_columns]
        
    #     self.tickers = pd.Series(self.holdings[self.ticker_col].unique())
        

    def clean_tickers(self):
        if len(self.tickers):
            exclude_list = ['SHV','SPAXX**','CORE**',np.nan]
            self.tickers = self.tickers[~self.tickers.isin(exclude_list)]
            
            replace_list = {"GOOG":'GOOGL'}
            for r in replace_list:
                self.tickers[self.tickers==r] = replace_list[r]
                self.holdings[self.ticker_col][self.holdings[self.ticker_col]==r] = replace_list[r]
            
        else:
            raise Warning('tickers variable not set')
 
    def explode_etfs(self,etf_holdings_paths):
        if len(self.holdings):
            # add col to track exploded etfs
            self.holdings['From Exploded ETF'] = False
            
            #find ETFs
            quandl.ApiConfig.api_key = self.quandl_api_key
            ticker_data = quandl.get_table('SHARADAR/TICKERS', ticker=self.tickers.str.cat(sep=','))
            etf_tickers = ticker_data[ticker_data.category=='ETF'].ticker
            
            etf_holdings = {}
            for e in etf_tickers:
                if e in etf_holdings_paths.keys():                   
                    file_type = etf_holdings_paths[e].split(sep='.')[-1] 
                    if file_type == 'csv':
                        etf_holdings[e] = pd.read_csv(etf_holdings_paths[e])
                    elif file_type == 'xlsx':
                        etf_holdings[e] = pd.read_excel(etf_holdings_paths[e])
                    else:
                        raise Warning('file type not supported for ' + str(etf_holdings_paths[e]))
                else:
                    raise Warning('no path provided for ETF:' + str(e))
                
                # find columns for ticker, stock name, weights
                possible_ticker_cols = ['Ticker','ticker']
                possible_name_cols = ['Name']
                possible_weights_cols = ['Weight','Weight (%)']
                
                ticker_col = str(etf_holdings[e].columns[etf_holdings[e].columns.isin(possible_ticker_cols)][0])
                if len(ticker_col) == 0:
                    raise Warning('Ticker column not found in holdings file for etf: ' +str(e))                
                
                name_col = str(etf_holdings[e].columns[etf_holdings[e].columns.isin(possible_name_cols)][0])
                if len(name_col) == 0:
                    raise Warning('Name column not found in holdings file for etf: ' +str(e))
                        
                weights_col = str(etf_holdings[e].columns[etf_holdings[e].columns.isin(possible_weights_cols)][0])
                if len(weights_col) == 0:
                    raise Warning('Weights column not found in holdings file for etf: ' +str(e))
                
                etf_holdings[e] = etf_holdings[e][[ticker_col,name_col,weights_col]]
                etf_holdings[e].columns = [self.ticker_col ,self.name_col,self.value_col]
                
                for i in self.holdings[self.holdings[self.ticker_col]==e].index:
                    # rescale weights to sum to zero then weight by portfolio weight
                    etf_holdings[e][self.value_col] = etf_holdings[e][self.value_col]/etf_holdings[e][self.value_col].sum()
                    dollar_in_ptf = self.holdings[self.holdings[self.ticker_col]==e][self.value_col][i]
                    etf_holdings[e][self.value_col] = etf_holdings[e][self.value_col]*dollar_in_ptf
                    
                    etf_holdings[e][self.account_col] = self.holdings[self.holdings[self.ticker_col]==e][self.account_col][i]
                    
                    #append to holdings and delete etf line
                    etf_holdings[e]['From Exploded ETF'] = True
                    self.holdings = self.holdings.append(etf_holdings[e])
                    self.holdings = self.holdings.drop(index=i)
        
            self.tickers = pd.Series(self.holdings[self.ticker_col].unique())            
        else:
            raise Warning('no holdings added yet')
        
     
    def get_fundamentals(self,date,dimension,table='SHARADAR/SF1'):
        #TODO: add error handling for quandl API
        quandl.ApiConfig.api_key = self.quandl_api_key
        self.fundamentals = quandl.get_table(table, calendardate=date, ticker=self.tickers.str.cat(sep=','))
        self.fundamentals = self.fundamentals[self.fundamentals.dimension==dimension]
        
        tickers_fund = self.fundamentals.ticker.unique()
        self.tickers_w_no_fund = self.tickers[~self.tickers.isin(tickers_fund)]
    
    def get_metadata(self,table='SHARADAR/TICKERS'):
        #TODO: add error handling for quandl API
        quandl.ApiConfig.api_key = self.quandl_api_key
        self.metadata = quandl.get_table(table, ticker=self.tickers.str.cat(sep=','))
        
    def merge_holdings_fundamentals(self):
        #merge fundamentals and holings
        left_on = self.ticker_col
        right_on = 'ticker' #name of ticker in quandl
        self.df = self.holdings.merge(self.fundamentals,how='inner',left_on=left_on,right_on=right_on)
        #merge with metadata
        self.df = self.df.merge(self.metadata,how='inner',on='ticker')
        
        #Check
        expected_tickers = pd.Series(self.tickers[~self.tickers.isin(self.tickers_w_no_fund)]).sort_values()
        returned_tickers = pd.Series(self.df.ticker.unique()).sort_values()
        if not all(expected_tickers.values==returned_tickers.values):
            raise Warning("fundamentals tickers don't match holdings")
        
        self.df['weight'] = self.df[self.value_col]/self.df[self.value_col].sum()