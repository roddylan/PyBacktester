import numpy as np
import pandas as pd

# performance measures

def create_sharpe_ratio(returns, periods=252):
    '''
    Creates Sharpe ratio for strategiy, based 
    on a benchmark of zero (ie. no risk free rate info)
    
    Parameters:
    returns - pandas Series repr. period % returns
    periods - daily (default; 252), hourly, minutely, etc.
    '''
    return np.sqrt(periods) * (np.mean(returns)) / np.std(returns)

def create_drawdowns(pnl):
    '''
    Calculate the largest peak-to-trough drawdown of the 
    profit and loss (PnL) curve as well as the duration
    of the drawdown. Requires that the pnl_returns is a 
    pd.Series object
    
    Parameters:
    pnl - pd.Series representing period % returns

    Returns:
    drawdown, duration - highest peak-to-trough drawdown and duration
    '''

    # calc. cumulative returns curve + set up High Water Mark
    hwm = [0]
    
    # drawdown + duration series
    idx = pnl.index
    drawdown, duration = pd.Series(index=idx), pd.Series(index=idx)

    # loop over idx range
    for i in range(1, len(idx)):
        hwm.append(max(hwm[i-1], pnl[i]))
        drawdown[i] = (hwm[i] - pnl[i])
        duration[i] = (0 if drawdown[i] == 0 else duration[i-1] + 1)
    
    return drawdown, drawdown.max(), duration.max()