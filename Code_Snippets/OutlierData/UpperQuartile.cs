 public async Task<double[]> GetUpperQuartileByDateQuerySet(string startDate, string endDate)
        {
            double uqPM = await this.GetUpperQuartileByDatePM(startDate, endDate);
            double uqRH = await this.GetUpperQuartileByDateRHDP(startDate, endDate, "Relative Humidity");
            double uqDP = await this.GetUpperQuartileByDateRHDP(startDate, endDate, "Dew Point");

            double[] uqArray = new double[] { uqPM, uqRH, uqDP };
            return uqArray;
        }

        private async Task<double> GetUpperQuartileByDatePM(string startDate, string endDate)
        {
            Container pmContainer = await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);

            Query q = new Query();
            double uq = 0.0;

            //string the queries together
            //first, get the # of "rows"/objects for the TOP command)
            Utilities.PrintParams(new string[2] { startDate, endDate });
            QueryDefinition queryDefinition = new QueryDefinition(q.totalEntryCountByDate)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
            var entryMidpoint = (await this.RunQueryWithParams(pmContainer, queryDefinition));
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(entryMidpoint[0].ToString());
            double quarterJsonResult = ((jsonResult["$1"] * 1.0) / 4) * 3;
            //Console.WriteLine(quarterJsonResult);
            int rowNum = (int)jsonResult["$1"] - (int)quarterJsonResult;
            //then, find midpoints and calculate
            if (rowNum % 2 == 0) //even # of objects
            {

                Console.WriteLine("************* EVEN # OBJECTS *********\n");
                
                double uq1 = (rowNum);
                double uq2 = (rowNum + 1); //the middle objects, for reasons i dont understand this rounds down, so correcting....

                Utilities.PrintParams(new string[3] { startDate, endDate, "" + uq1 });
                queryDefinition = new QueryDefinition(q.getMedianLowerValue)
                    .WithParameter("@rowNum", uq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result1 = await this.RunQueryWithParams(pmContainer, queryDefinition);


                Utilities.PrintParams(new string[3] { startDate, endDate, "" + uq2 });
                queryDefinition = new QueryDefinition(q.getMedianLowerValue)
                    .WithParameter("@rowNum", uq2)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result2 = await this.RunQueryWithParams(pmContainer, queryDefinition);

                //get the two midpoints
                //finds the highest value of this subset of data. MAX of ASC data
                double md1 = FindLowestValue(result1);
                //finds the lowest value of this subset of data. MIN of DESC data
                double md2 = FindLowestValue(result2);

                Console.WriteLine("median1: " + md1);
                Console.WriteLine("median2: " + md2);

                uq = (md1 + md2) / 2;
                Console.WriteLine("Upper Quartile PM2.5 Read for dates " + startDate + " - " + endDate + ": " + uq);

            }
            else // odd # of objects
            {

                Console.WriteLine("********* ODD # OBJECTS ************\n");

                var uq1 = rowNum; //the middle object, for reasons i dont understand it rounds down

                //simplified since we dont need to average 2 numbers
                Utilities.PrintParams(new string[3] { startDate, endDate, "" + uq1 });
                queryDefinition = new QueryDefinition(q.getMedianLowerValue)
                    .WithParameter("@rowNum", uq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result = await this.RunQueryWithParams(pmContainer, queryDefinition);


                //finds the lowest value of this subset of data. MIN of DESC data
                //because we only need the one, no additional arithmetic is needed.
                uq = FindLowestValue(result);

                Console.WriteLine("Upper Quartile PM2.5 Read for dates " + startDate + " - " + endDate + ": " + uq);
            }
            return uq;
        }

        private async Task<double> GetUpperQuartileByDateRHDP(string startDate, string endDate, string typeOfMeasure)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);


            Query q = new Query();
            double uq = 0.0;

            //string the queries together
            //first, get the # of "rows"/objects for the TOP command)
            Utilities.PrintParams(new string[3] { startDate, endDate, typeOfMeasure });
            QueryDefinition queryDefinition = new QueryDefinition(q.totalEntryCountByDateRHDP)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
            var entryMidpoint = (await this.RunQueryWithParams(rhContainer, queryDefinition));
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(entryMidpoint[0].ToString());
            double quarterJsonResult = ((jsonResult["$1"] * 1.0) / 4) * 3;
            int rowNum = (int)jsonResult["$1"] - (int)quarterJsonResult;
            //then, find midpoints and calculate
            if (rowNum % 2 == 0) //even # of objects
            {

                Console.WriteLine("************* EVEN # OBJECTS *********\n");

                double uq1 = rowNum;
                double uq2 = rowNum + 1; //the middle objects, for reasons i dont understand this rounds down, so correcting....

                Utilities.PrintParams(new string[4] { startDate, endDate, "" + uq1, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianLowerValueRHDP)
                    .WithParameter("@rowNum", uq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result1 = await this.RunQueryWithParams(rhContainer, queryDefinition);


                Utilities.PrintParams(new string[4] { startDate, endDate, "" + uq2, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianLowerValueRHDP)
                    .WithParameter("@rowNum", uq2)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result2 = await this.RunQueryWithParams(rhContainer, queryDefinition);

                //get the two midpoints
                //finds the highest value of this subset of data. MAX of ASC data
                double md1 = FindLowestValue(result1);
                //finds the lowest value of this subset of data. MIN of DESC data
                double md2 = FindLowestValue(result2);

                Console.WriteLine("median1: " + md1);
                Console.WriteLine("median2: " + md2);

                uq = (md1 + md2) / 2;
                Console.WriteLine("Upper Quartile "+typeOfMeasure+" Read for dates " + startDate + " - " + endDate + ": " + uq);

            }
            else // odd # of objects
            {

                Console.WriteLine("********* ODD # OBJECTS ************\n");

                var uq1 = rowNum; //the middle object, for reasons i dont understand it rounds down

                //simplified since we dont need to average 2 numbers
                Utilities.PrintParams(new string[4] { startDate, endDate, "" + uq1, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianLowerValueRHDP)
                    .WithParameter("@rowNum", uq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result = await this.RunQueryWithParams(rhContainer, queryDefinition);


                //finds the highest value of this subset of data. MAX of ASC data
                //because we only need the one, no additional arithmetic is needed.
                uq = FindLowestValue(result);

                Console.WriteLine("Upper Quartile "+typeOfMeasure+" Read for dates " + startDate + " - " + endDate + ": " + uq);
            }
            return uq;
        }

    public string totalEntryCountByDate { get; } = @"
        SELECT COUNT(c.id) 
        FROM c 
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string totalEntryCountByDateRHDP { get; } = @"
        SELECT COUNT(c.id)
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";
    
    public string getMedianLowerValue { get; } = @"SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND  (c.dateGMT BETWEEN @startDate AND @endDate)
        ORDER BY c.sampleMeasurement DESC";
    
    public string getMedianLowerValueRHDP { get; } = @"SELECT TOP @rowNum c.sampleMeasurement as median
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND  (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)
        ORDER BY c.sampleMeasurement DESC";