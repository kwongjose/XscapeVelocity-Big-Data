 public async Task GetMaxMeasurePerYearQuerySet(string startDate, string endDate)
        {
            await this.GetMaxMeasurePerYearPM(startDate, endDate);
            await this.GetMaxMeasurePerYearRHDP(startDate, endDate, "Relative Humidity");
            await this.GetMaxMeasurePerYearRHDP(startDate, endDate, "Dew Point");
        }

        private async Task GetMaxMeasurePerYearPM(string startDate, string endDate)
        {
            Container pmContainer = await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[2] { startDate, endDate });
            QueryDefinition queryDefinition = new QueryDefinition(q.maxSampleMeasurementByYear)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
            var result = await this.RunQueryWithParams(pmContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double max = jsonResult["maxMeasure"];

            Console.WriteLine("Maximum PM2.5 Read for dates " + startDate + " - " + endDate + ": " + max);
        }

        private async Task GetMaxMeasurePerYearRHDP(string startDate, string endDate, string typeOfMeasure)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[3] { startDate, endDate, typeOfMeasure });
            QueryDefinition queryDefinition = new QueryDefinition(q.maxSampleMeasurementByYearRHDP)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
            var result = await this.RunQueryWithParams(rhContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double max = jsonResult["maxMeasure"];

            Console.WriteLine("Maximum "+typeOfMeasure+" Read for dates " + startDate + " - " + endDate + ": " + max);
        }

    public string maxSampleMeasurementByYear { get; } = @"
        SELECT MAX(c.sampleMeasurement) as maxMeasure
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate)";

    public string maxSampleMeasurementByYearRHDP { get; } = @"
        SELECT MAX(c.sampleMeasurement) as maxMeasure
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure)";
