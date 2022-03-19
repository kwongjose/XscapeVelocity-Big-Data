public async Task GetDifferenceSinceCovid()
        {

            double[] preCovidMedian = await this.GetMedianPerYearQuerySet("2017-01-01", "2020-03-01");
            double[] postCovidMedian = await this.GetMedianPerYearQuerySet("2020-03-01", "2020-12-31");
            for (int i = 0; i < preCovidMedian.Length; i++)
            {
                double diffMedian = preCovidMedian[i] - postCovidMedian[i];
                if (i % 3 == 0) //pm
                {
                    Console.WriteLine("Pre-Covid Mean PM2.5 Read is " + preCovidMedian[i]);
                    Console.WriteLine("Post-Covid Mean PM2.5 Read is " + postCovidMedian[i]);
                    Console.WriteLine("The difference in PM2.5 Read data is " + diffMedian);
                }
                else if (i % 3 == 1) //rh
                {
                    Console.WriteLine("Pre-Covid Median Relative Humidity Read is " + preCovidMedian[i]);
                    Console.WriteLine("Post-Covid Median Relative Humidity Read is " + postCovidMedian[i]);
                    Console.WriteLine("The difference in Relative Humidity Read data is " + diffMedian);
                }
                else if (i % 3 == 2) //three is dewpoint
                {
                    Console.WriteLine("Pre-Covid Median Dew Point Read is " + preCovidMedian[i]);
                    Console.WriteLine("Post-Covid Median Dew Point Read is " + postCovidMedian[i]);
                    Console.WriteLine("The difference in Dew Point Read data is " + diffMedian);
                }
            }
            double[] preCovidMean = await this.GetAverageMeasurePerYearQuerySet("2017-01-01", "2020-03-01");
            double[] postCovidMean = await this.GetAverageMeasurePerYearQuerySet("2020-03-01", "2020-12-31");
            for(int i = 0; i < preCovidMean.Length; i++)
            {
                double diffMean = preCovidMean[i] - postCovidMean[i];
                if (i % 3 == 0) //pm
                {
                    Console.WriteLine("Pre-Covid Mean PM2.5 Read is " + preCovidMean[i]);
                    Console.WriteLine("Post-Covid Mean PM2.5 Read is " + postCovidMean[i]);
                    Console.WriteLine("The difference in PM2.5 Read data is " + diffMean);
                } else if (i % 3 == 1) //rh
                {
                    Console.WriteLine("Pre-Covid Mean Relative Humidity Read is " + preCovidMean[i]);
                    Console.WriteLine("Post-Covid Mean Relative Humidity Read is " + postCovidMean[i]);
                    Console.WriteLine("The difference in Relative Humidity Read data is " + diffMean);
                } else if (i % 3 == 2) //three is dewpoint
                {
                    Console.WriteLine("Pre-Covid Mean Dew Point Read is " + preCovidMean[i]);
                    Console.WriteLine("Post-Covid Mean Dew Point Read is " + postCovidMean[i]);
                    Console.WriteLine("The difference in Dew Point Read data is " + diffMean);
                }
            }
            
        }