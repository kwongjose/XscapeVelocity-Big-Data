        public async Task<double[]> GetMedianPerYearQuerySet(string startDate, string endDate)
        {
            double median = await this.GetMedianPerYearPM2(startDate, endDate);
            double rhmedian = await this.GetMedianPerYearRHDP(startDate, endDate, "Relative Humidity");
            double dpmedian = await this.GetMedianPerYearRHDP(startDate, endDate, "Dew Point");

            double[] medianArray = new double[] { median, rhmedian, dpmedian };

            return medianArray;
        }

         private async Task<double> GetMedianPerYearPM2(string startDate, string endDate)
        {
            Container pmContainer = await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);

            Query q = new Query();
            double median = 0.0;

            //string the queries together
            //first, get the # of "rows"/objects for the TOP command)
            Utilities.PrintParams(new string[2] { startDate, endDate });
            QueryDefinition queryDefinition = new QueryDefinition(q.totalEntryCountByDate)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
            var entryMidpoint = (await this.RunQueryWithParams(pmContainer, queryDefinition));
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(entryMidpoint[0].ToString());

            //then, find midpoints and calculate
            if (jsonResult["$1"] % 2 == 0) //even # of objects
            {

                Console.WriteLine("************* EVEN # OBJECTS *********\n");

                double midPoint1 = ((jsonResult["$1"] + 1) / 2);
                double midPoint2 = (jsonResult["$1"] / 2 + 1); //the middle objects, for reasons i dont understand this rounds down, so correcting....

                Utilities.PrintParams(new string[3] { startDate, endDate, "" + midPoint1 });
                queryDefinition = new QueryDefinition(q.getMedianUpperValue)
                    .WithParameter("@rowNum", midPoint1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result1 = await this.RunQueryWithParams(pmContainer, queryDefinition);


                Utilities.PrintParams(new string[3] { startDate, endDate, "" + midPoint2 });
                queryDefinition = new QueryDefinition(q.getMedianLowerValue)
                    .WithParameter("@rowNum", midPoint2)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result2 = await this.RunQueryWithParams(pmContainer, queryDefinition);

                //get the two midpoints
                //finds the highest value of this subset of data. MAX of ASC data
                double md1 = FindHighestValue(result1);
                //finds the lowest value of this subset of data. MIN of DESC data
                double md2 = FindLowestValue(result2);

                Console.WriteLine("median1: " + md1);
                Console.WriteLine("median2: " + md2);

                median = (md1 + md2) / 2;
                Console.WriteLine("Median PM2.5 Read for dates " + startDate + " - " + endDate + ": " + median);

            }
            else // odd # of objects
            {

                Console.WriteLine("********* ODD # OBJECTS ************\n");

                var midPoint = (jsonResult["$1"] + 1) / 2; //the middle object, for reasons i dont understand it rounds down

                //simplified since we dont need to average 2 numbers
                Utilities.PrintParams(new string[3] { startDate, endDate, "" + midPoint });
                queryDefinition = new QueryDefinition(q.getMedianUpperValue)
                    .WithParameter("@rowNum", midPoint)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result = await this.RunQueryWithParams(pmContainer, queryDefinition);


                //finds the highest value of this subset of data. MAX of ASC data
                //because we only need the one, no additional arithmetic is needed.
                median = FindHighestValue(result);

                Console.WriteLine("Median PM2.5 Read for dates " + startDate + " - " + endDate + ": " + median);
            }
            return median;
        }

        private async Task<double> GetMedianPerYearRHDP(string startDate, string endDate, string typeOfMeasure)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);

            Query q = new Query();
            double median = 0.0;

            //string the queries together
            //first, get the # of "rows"/objects for the TOP command)
            Utilities.PrintParams(new string[3] { startDate, endDate, typeOfMeasure });
            QueryDefinition queryDefinition = new QueryDefinition(q.totalEntryCountByDate)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
            var entryMidpoint = (await this.RunQueryWithParams(rhContainer, queryDefinition));
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(entryMidpoint[0].ToString());

            //then, find midpoints and calculate
            if (jsonResult["$1"] % 2 == 0) //even # of objects
            {

                Console.WriteLine("************* EVEN # OBJECTS *********\n");

                double midPoint1 = ((jsonResult["$1"] + 1) / 2);
                double midPoint2 = (jsonResult["$1"] / 2 + 1); //the middle objects, for reasons i dont understand this rounds down, so correcting....

                Utilities.PrintParams(new string[4] { startDate, endDate, "" + midPoint1, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianUpperValueRHDP)
                    .WithParameter("@rowNum", midPoint1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result1 = await this.RunQueryWithParams(rhContainer, queryDefinition);


                Utilities.PrintParams(new string[4] { startDate, endDate, "" + midPoint2, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianLowerValueRHDP)
                    .WithParameter("@rowNum", midPoint2)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result2 = await this.RunQueryWithParams(rhContainer, queryDefinition);

                //get the two midpoints
                //finds the highest value of this subset of data. MAX of ASC data
                double md1 = FindHighestValue(result1);
                //finds the lowest value of this subset of data. MIN of DESC data
                double md2 = FindLowestValue(result2);

                Console.WriteLine("median1: " + md1);
                Console.WriteLine("median2: " + md2);

                median = (md1 + md2) / 2;
                Console.WriteLine("Median " + typeOfMeasure + " Read for dates " + startDate + " - " + endDate + ": " + median);

            }
            else // odd # of objects
            {

                Console.WriteLine("********* ODD # OBJECTS ************\n");

                var midPoint = (jsonResult["$1"] + 1) / 2; //the middle object, for reasons i dont understand it rounds down

                //simplified since we dont need to average 2 numbers
                Utilities.PrintParams(new string[4] { startDate, endDate, "" + midPoint, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianUpperValue)
                    .WithParameter("@rowNum", midPoint)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result = await this.RunQueryWithParams(rhContainer, queryDefinition);


                //finds the highest value of this subset of data. MAX of ASC data
                //because we only need the one, no additional arithmetic is needed.
                median = FindHighestValue(result);

                Console.WriteLine("Median " + typeOfMeasure + " Read for dates " + startDate + " - " + endDate + ": " + median);
            }
            return median;
        }

    public string totalEntryCountByDate { get; } = @"
        SELECT COUNT(c.id) 
        FROM c 
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string totalEntryCountByDateRHDP { get; } = @"
        SELECT COUNT(c.id)
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";
    
    public string getMedianUpperValue { get; } = @"
        SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)
        ORDER BY c.sampleMeasurement ASC";
    
    public string getMedianUpperValueRHDP { get; } = @"
        SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)
        ORDER BY c.sampleMeasurement ASC";

    public string getMedianLowerValue { get; } = @"SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND  (c.dateGMT BETWEEN @startDate AND @endDate)
        ORDER BY c.sampleMeasurement DESC";
    
    public string getMedianLowerValueRHDP { get; } = @"SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND  (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)
        ORDER BY c.sampleMeasurement DESC";
