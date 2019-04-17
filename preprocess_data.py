import pandas as pd
import numpy as np


def make_fraud_col(df_in):
    fraud_accts = set(['fraudster_event', 'fraudster', 'fraudster_att'])
    
    new_df = df_in.copy()
    new_df['fraudulent'] = df_in['acct_type'].apply(lambda x: 1 if x in fraud_accts else 0)
    new_df.drop('acct_type', axis=1, inplace=True)
    return new_df

def ticket_quant_calc(df_in): 
    try:
        #Get ticket type totals for each event
        ticket_quant_totals = ([sum([i['quantity_total'] for i in x]) for x in df_in['ticket_types']])
        #Get ticket type sold totals for each even
        ticket_sold_totals = ([sum([i['quantity_sold'] for i in x]) for x in df_in['ticket_types']])
    
        #make new data frame of totals and calculate ratios between the two
        ticket_types = pd.DataFrame({'ticket_quant_totals': ticket_quant_totals, 'ticket_sold_totals': ticket_sold_totals})
        ticket_types['total_sold_ratio'] = ticket_types['ticket_sold_totals']/ticket_types['ticket_quant_totals']
        
        #merge new data frame with original
        new_df = pd.concat([df_in, ticket_types], axis=1)
        return new_df
    
    except:
        print('\nCouldn\'t apply ticket_quant_calc')
        df_in['total_sold_ratio'] = 0
        return df_in
        

def mask_payouts(df_in):
    try:
        new_df = df_in.copy()
        new_df['previous_payouts'] = new_df.previous_payouts.str.len() > 0
        new_df['previous_payouts'] = ((new_df.previous_payouts+1)-1)
        return new_df
    
    except:
        print('\nCouldn\'t apply mask_payouts')
        new_df['previous_payouts'] = 0
        return df_in

def pre_process_nan(df_in):
    try:
        #Trash sale duration outliers
        df_in = df_in.drop(df_in[df_in.sale_duration < 0].index)
        #drop nulls in country
        df_in = df_in.dropna(subset=['country'])
        #fill nulls in total sold ratios with 0's
        df_in.total_sold_ratio=df_in.total_sold_ratio.fillna(0.0)
        #fill nulls in sale duration with mean sale duration vals
        df_in.sale_duration=df_in.sale_duration.fillna(df_in.sale_duration.mean())
    
        #replace infinities in total_sold_ratio with 0's
        df_in = df_in.replace([np.inf, -np.inf], 0)
        return df_in
    
    except:
        print('\nCouldn\'t apply pre_process_nan')
        df_in.fillna(0, inplace=True)
        df_in = df_in.replace([np.inf, -np.inf], 0)
        return df_in

def yank_columns(df_in):
    return df_in[['fraudulent', 'body_length', 'user_age', 'channels', 'sale_duration', 'num_payouts', 'has_analytics', 'previous_payouts','total_sold_ratio','gts']]

def blind_yank_columns(df_in):
        return df_in[['body_length', 'user_age', 'channels', 'sale_duration', 'num_payouts', 'has_analytics', 'previous_payouts','total_sold_ratio','gts']]

def pipeline(df_in):
    '''
    Applies all of the above transformations to a DataFrame
    Returns the transformed DataFrame
    '''
    return (df_in.pipe( make_fraud_col )
                 .pipe( ticket_quant_calc )
                 .pipe( mask_payouts )
                 .pipe( pre_process_nan )
                 .pipe( yank_columns )
            )

def blind_pipeline(df_in):
    '''
    Does the same as above, except for the live data which doesn't have the acct_type column
    '''
    return (df_in.pipe( ticket_quant_calc )
                 .pipe( mask_payouts )
                 .pipe( pre_process_nan )
                 .pipe( blind_yank_columns )
            )