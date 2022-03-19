public async Task GetOutliersByDateQuerySet(string startDate, string endDate, double[] lowerBoundArray, double[] upperBoundArray)
        {
            await this.GetOutliersByDatePM(startDate, endDate, lowerBoundArray[0], upperBoundArray[0]);
            await this.GetOutliersByDateRHDP(startDate, endDate, lowerBoundArray[1], upperBoundArray[1], "Relative Humidity");
            await this.GetOutliersByDateRHDP(startDate, endDate, lowerBoundArray[2], upperBoundArray[2], "Dew Point");
        }

         private async Task GetOutliersByDatePM(string startDate, string endDate, double lb, double ub)
        {
            Container pmContainer = await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[] { startDate, endDate, ""+lb});
            QueryDefinition queryDefinition = new QueryDefinition(q.countOccurencesOfMeasurementLessThanByDatePM)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@measure", lb);

            var result = await this.RunQueryWithParams(pmContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double occ = jsonResult["numOfOccurences"];

            Console.WriteLine("Number of Lower Bound Outlier PM2.5 Reads: " + occ);

            Utilities.PrintParams(new string[] { startDate, endDate, "" + ub });
            queryDefinition = new QueryDefinition(q.countOccurencesOfMeasurementGreaterThanByDatePM)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@measure", ub);

             result = await this.RunQueryWithParams(pmContainer, queryDefinition);
             jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
             occ = jsonResult["numOfOccurences"];

            Console.WriteLine("Number of Upper Bound Outlier PM2.5 Reads: " + occ);

        }

        private async Task GetOutliersByDateRHDP(string startDate, string endDate, double lb, double ub, string typeOfMeasure)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[] { startDate, endDate, "" + lb, typeOfMeasure });
            QueryDefinition queryDefinition = new QueryDefinition(q.countOccurencesOfMeasurementLessThanByDateRHDP)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@measure", lb)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);

            var result = await this.RunQueryWithParams(rhContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double occ = jsonResult["numOfOccurences"];

            Console.WriteLine("Number of Lower Bound Outlier "+typeOfMeasure+" Reads: " + occ);

            Utilities.PrintParams(new string[] { startDate, endDate, "" + ub, typeOfMeasure });
            queryDefinition = new QueryDefinition(q.countOccurencesOfMeasurementGreaterThanByDateRHDP)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@measure", ub)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);

            result = await this.RunQueryWithParams(rhContainer, queryDefinition);
            jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            occ = jsonResult["numOfOccurences"];

            Console.WriteLine("Number of Upper Bound Outlier "+typeOfMeasure+" Reads: " + occ);
        }

    public string countOccurencesOfMeasurementLessThanByDatePM { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement < @measure AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string countOccurencesOfMeasurementLessThanByDateRHDP { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement < @measure AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";

    public string countOccurencesOfMeasurementGreaterThanByDatePM { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement > @measure AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string countOccurencesOfMeasurementGreaterThanByDateRHDP { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement > @measure AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";
