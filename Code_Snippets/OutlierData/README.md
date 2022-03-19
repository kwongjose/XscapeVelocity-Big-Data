# Table 5, 6: Finding Outlier Data

First we used a list of all possible dates and iterated through the queries to find the Lower Quartile range, the Upper Quartile range, and then finally search for outliers based on these bounds.
    
*  These are the code and query snippets within `Main.cs`

Then we calculated the number of entries that existed and determined what 1/4 of the total entries would be, querying for the "median" based on this number as our limiter. The query `GetMedianUpperValue` is reused here, as it functions the same way calculating the lower quartile would have. We perform this for all three measurements.

*  These are the code and query snippets within `LowerQuartile.cs`

Then we repeated the count of total entries and used a similar method to determine the upper quartile, including the reuse of the `GetMedianLowerValue` query. This sounds backwards, but with finding the Highest value of the Lower query, and highest value of the upper query, we found the correct bounds. 

*  These are the code and query snippets within `UpperQuartile.cs`

Then we used our calculated quartiles to determine the Interquartile Range, and queried to count for all measurements that exceeded our upper and lower bounds for each given year.

*  These are the code and query snippets within `Outlier.cs`

Finally, the bottom of each file are queries that were used.