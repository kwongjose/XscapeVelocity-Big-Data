# Table 2: Finding Minimum, Maximum, and Median Measurement

First we used a list of all possible dates and iterated through the queries for Median, Minimum, and Maximum, logically organized into distinct functions.
    
*  These are the code and query snippets within `Main.cs`

Then we get the median measure per year for PM2.5, Relative Humidity, and Dewpoint. This required multiple queries and then further calculations in the application code. Running locally, it is an extremely slow query (roughly 6 minutes).

*  These are the code and query snippets within `Median.cs`

Then we get the minimum measure per year for PM2.5, Relative Humidity, and Dewpoint.

*  These are the code and query snippets within `Minimum.cs`

Then we get the maximum measure per year for PM2.5, Relative Humidity, and Dewpoint.

*  These are the code and query snippets within `Maximum.cs`

Finally, the bottom of each file are queries that were used.