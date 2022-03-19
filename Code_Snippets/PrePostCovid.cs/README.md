# Figure <TODO> Finding the Difference in Measurement Before and After COVID-19 (Table)

First we determined which dates to use and iterated through the queries for Median and Average for both PM2.5, Relative Humidity, and Dew Point.
    
*  These are the code and query snippets within `Main.cs`

Then we get the median measure per date range for PM2.5, Relative Humidity, and Dewpoint. This required multiple queries and then further calculations in the application code. Running locally, it is an extremely slow query (roughly 6 minutes).

*  These are the code and query snippets within `Median.cs`

Then we get the average measure per date range for PM2.5, Relative Humidity, and Dewpoint.

*  These are the code and query snippets within `Average.cs`

Finally, the bottom of each file are queries that were used.