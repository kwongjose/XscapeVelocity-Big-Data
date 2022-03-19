        public async Task GetMinMeasurePerYearQuerySet(string startDate, string endDate)
        {
            await this.GetMinMeasurePerYearPM(startDate, endDate);
            await this.GetMinMeasurePerYearRHDP(startDate, endDate, "Relative Humidity");
            await this.GetMinMeasurePerYearRHDP(startDate, endDate, "Dew Point");
        }

 private async Task GetMinMeasurePerYearPM(string startDate, string endDate)
        {
            Container pmContainer = await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[2] { startDate, endDate });
            QueryDefinition queryDefinition = new QueryDefinition(q.minSampleMeasurementByYear)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
            var result = await this.RunQueryWithParams(pmContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double min = jsonResult["minMeasure"];

            Console.WriteLine("Minimum PM2.5 Read for dates " + startDate + " - " + endDate + ": " + min);
        }

        private async Task GetMinMeasurePerYearRHDP(string startDate, string endDate, string typeOfMeasure)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[3] { startDate, endDate, typeOfMeasure });
            QueryDefinition queryDefinition = new QueryDefinition(q.minSampleMeasurementByYearRHDP)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
            var result = await this.RunQueryWithParams(rhContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double min = jsonResult["minMeasure"];

            Console.WriteLine("Minimum "+typeOfMeasure+" Read for dates " + startDate + " - " + endDate + ": " + min);
        }

public string minSampleMeasurementByYear { get; } = @"
        SELECT MIN(c.sampleMeasurement) as minMeasure
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";

public string minSampleMeasurementByYearRHDP { get; } = @"
        SELECT MIN(c.sampleMeasurement) as minMeasure
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";