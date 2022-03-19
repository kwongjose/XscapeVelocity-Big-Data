        public async Task<double[]> GetLowerQuartileByDateQuerySet(string startDate, string endDate)
        {
            double lqPM = await this.GetLowerQuartileByDatePM(startDate, endDate);
            double lqRH = await this.GetLowerQuartileByDateRHDP(startDate, endDate, "Relative Humidity");
            double lqDP = await this.GetLowerQuartileByDateRHDP(startDate, endDate, "Dew Point");
           

            double[] lqArray = new double[] { lqPM, lqRH, lqDP };
            return lqArray;
        }

        private async Task<double> GetLowerQuartileByDatePM(string startDate, string endDate)
        {
            Container pmContainer = await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);

            Query q = new Query();
            double lq = 0.0;

            //string the queries together
            //first, get the # of "rows"/objects for the TOP command)
            Utilities.PrintParams(new string[2] { startDate, endDate });
            QueryDefinition queryDefinition = new QueryDefinition(q.totalEntryCountByDate)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
            var entryMidpoint = (await this.RunQueryWithParams(pmContainer, queryDefinition));
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(entryMidpoint[0].ToString());
            //then, find midpoints and calculate
            double quarterJsonResult = (jsonResult["$1"] * 1.0) / 4;
            int intJsonResult = (int)quarterJsonResult;
            if (intJsonResult % 2 == 0) //even # of objects
            {

                Console.WriteLine("************* EVEN # OBJECTS *********\n");

                double lq1 = (intJsonResult + 1);
                double lq2 = (intJsonResult);

                Utilities.PrintParams(new string[3] { startDate, endDate, "" + lq1 });
                queryDefinition = new QueryDefinition(q.getMedianUpperValue)
                    .WithParameter("@rowNum", lq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result1 = await this.RunQueryWithParams(pmContainer, queryDefinition);


                Utilities.PrintParams(new string[3] { startDate, endDate, "" + lq2 });
                queryDefinition = new QueryDefinition(q.getMedianUpperValue)
                    .WithParameter("@rowNum", lq2)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result2 = await this.RunQueryWithParams(pmContainer, queryDefinition);

                //get the two midpoints
                //finds the highest value of this subset of data. MAX of ASC data
                double md1 = FindHighestValue(result1);
                double md2 = FindHighestValue(result2);

                Console.WriteLine("median1: " + md1);
                Console.WriteLine("median2: " + md2);

                lq = (md1 + md2) / 2;
                Console.WriteLine("Lower Quartile PM2.5 Read for dates " + startDate + " - " + endDate + ": " + lq);

            }
            else // odd # of objects
            {

                Console.WriteLine("********* ODD # OBJECTS ************\n");

                int lq1 = intJsonResult ; //the middle object, casting int will truncate + round down

                //simplified since we dont need to average 2 numbers
                Utilities.PrintParams(new string[3] { startDate, endDate, "" + lq1 });
                queryDefinition = new QueryDefinition(q.getMedianUpperValue)
                    .WithParameter("@rowNum", lq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result = await this.RunQueryWithParams(pmContainer, queryDefinition);


                //finds the highest value of this subset of data. MAX of ASC data
                //because we only need the one, no additional arithmetic is needed.
                lq = FindHighestValue(result);

                Console.WriteLine("Lower Quartile PM2.5 Read for dates " + startDate + " - " + endDate + ": " + lq);
            }
            return lq;
        }

 private async Task<double> GetLowerQuartileByDateRHDP(string startDate, string endDate, string typeOfMeasure)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);

            Query q = new Query();
            double lq = 0.0;

            //string the queries together
            //first, get the # of "rows"/objects for the TOP command)
            Utilities.PrintParams(new string[3] { startDate, endDate, typeOfMeasure });
            QueryDefinition queryDefinition = new QueryDefinition(q.totalEntryCountByDateRHDP)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
            var entryMidpoint = (await this.RunQueryWithParams(rhContainer, queryDefinition));
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(entryMidpoint[0].ToString());
            double quarterJsonResult = (jsonResult["$1"] * 1.0) / 4;
            int intJsonResult = (int)quarterJsonResult;

            //then, find midpoints and calculate
            if (intJsonResult % 2 == 0) //even # of objects
            {

                Console.WriteLine("************* EVEN # OBJECTS *********\n");

                double lq1 = (intJsonResult + 1);
                double lq2 = intJsonResult;

                Utilities.PrintParams(new string[4] { startDate, endDate, "" + lq1, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianUpperValueRHDP)
                    .WithParameter("@rowNum", lq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result1 = await this.RunQueryWithParams(rhContainer, queryDefinition);


                Utilities.PrintParams(new string[4] { startDate, endDate, "" + lq2, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianUpperValueRHDP)
                    .WithParameter("@rowNum", lq2)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result2 = await this.RunQueryWithParams(rhContainer, queryDefinition);

                //get the two midpoints
                //finds the highest value of this subset of data. MAX of ASC data
                double md1 = FindHighestValue(result1);
                //finds the lowest value of this subset of data. MIN of DESC data
                double md2 = FindHighestValue(result2);

                Console.WriteLine("median1: " + md1);
                Console.WriteLine("median2: " + md2);

                lq = (md1 + md2) / 2;
                Console.WriteLine("Lower Quartile "+ typeOfMeasure + " Read for dates " + startDate + " - " + endDate + ": " + lq);

            }
            else // odd # of objects
            {

                Console.WriteLine("********* ODD # OBJECTS ************\n");

                var lq1 = intJsonResult; //the middle object, for reasons i dont understand it rounds down

                //simplified since we dont need to average 2 numbers
                Utilities.PrintParams(new string[4] { startDate, endDate, "" + lq1, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianUpperValueRHDP)
                    .WithParameter("@rowNum", lq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result = await this.RunQueryWithParams(rhContainer, queryDefinition);


                //finds the highest value of this subset of data. MAX of ASC data
                //because we only need the one, no additional arithmetic is needed.
                lq = FindHighestValue(result);

                Console.WriteLine("Lower Quartile "+ typeOfMeasure + " Read for dates " + startDate + " - " + endDate + ": " + lq);
            }
            return lq;
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