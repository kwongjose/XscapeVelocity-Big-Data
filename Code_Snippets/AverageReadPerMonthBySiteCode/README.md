# Figure <TODO> Finding Average Measurement Per Month (Line Graph)

First we used a list of all possible site codes and verified if the site code had an entry before attempting to measure averages.
    
*  These are the code and query snippets within `SiteCodeHasEntries.cs`

Then we get the average measure per month for all sites that have entries.

The average measure checks based on the type of measure, getting results for PM2.5, Relative Humidity, and Dewpoint.

* These are the code and query snippets within `AverageByMonthAndSiteCode.cs`

Finally, the bottom of each file are queries that were used.