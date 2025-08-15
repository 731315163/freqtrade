

import math
import numpy as np
import pandas as pd
def equal(a,b):

    
    if np.isscalar(a) and np.isscalar(b):
        
        if isinstance(a,(float,np.floating)) and isinstance(b,(float,np.floating)):
           if np.isnan(a) and np.isnan( b):
            return True
           return bool( np.isclose(a,b))
        else:
            return bool(a==b)
        
    elif np.isscalar(a) or np.isscalar(b):
        return False
    else:
        
        return bool(np.array_equal(a,b,True))

  