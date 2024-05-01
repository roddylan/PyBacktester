import datetime
from math import floor
import queue

import numpy as np
import pandas as pd

from event import FillEvent, OrderEvent
from performance import create_sharpe_ratio, create_drawdowns

# gen orders based on signals
# TODO: add Kelly Criterion

# init capital value =100k USD default
