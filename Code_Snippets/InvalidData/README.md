# Table 4, Invalid Data

First we queried PM2.5 data and then we queried Relative Humidity and Dew Point, all for negative measurements.
    
*  These are the code and query snippets within `InvalidData.cs`

Additional error handling for invalid figures such as non-number values and null values were not present in the data. This was confirmed during the load process of the data.

Finally, the bottom of each file are queries that were used.