        public async Task GetMonthlyAvgBySiteCode(List<string> sites, string[] datesByMonth)
        {
            for (int j = 0; j < sites.Count; j++)
            {
                List<string> dates = new List<string>();
                double[] resultsPerSiteCode = new double[] { };
                bool hasEntries = false;
                for (int i = 0; i < datesByMonth.Length; i++)
                {
                    hasEntries = await this.SiteCodeHasEntries(datesByMonth[i], datesByMonth[i + 1], sites[j]);
                    dates = new List<string>(); //empty
                    resultsPerSiteCode = new double[] { }; //empty each iteration
                    if (hasEntries)
                    {
                        resultsPerSiteCode = await this.GetAverageMeasurePerDateAndSiteQuerySet(datesByMonth[i], datesByMonth[i + 1], sites[j]);
                        dates.Add(datesByMonth[i] + "-" + datesByMonth[i + 1]);
                        Utilities.WriteToCSV(sites[j], dates, "mean", resultsPerSiteCode);
                    }
                    //skip 1 because every other is an end date
                    i++;
                }

            }
        }
        
        public async Task<double[]> GetAverageMeasurePerDateAndSiteQuerySet(string startDate, string endDate, string siteCode)
        {

            double mean = await this.GetAvgMeasureBySiteCodePM(startDate, endDate, siteCode);
            double rhmean = await this.GetAvgMeasureBySiteCodeRHDP(startDate, endDate, siteCode, "Relative Humidity");
            double dpmean = await this.GetAvgMeasureBySiteCodeRHDP(startDate, endDate, siteCode, "Dew Point");

            double[] meanArray = new double[] { mean, rhmean, dpmean };

            return meanArray;
        }



                private async Task<double> GetAvgMeasureBySiteCodePM(string startDate, string endDate, string siteCode)
        {
            Container pmContainer = await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);

            Query q = new Query();

            //Utilities.PrintParams(new string[3] { startDate, endDate, siteCode });
            QueryDefinition queryDefinition = new QueryDefinition(q.averageSampleMeasurementByDateAndSiteCode)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@siteCode", siteCode);
            var result = await this.RunQueryWithParams(pmContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double mean = 0.0;
            if (jsonResult.ContainsKey("average"))
            {
                mean = jsonResult["average"];
            }

                Console.WriteLine("Average/Mean PM2.5 Read for site " + siteCode + " on dates " + startDate + " - " + endDate + ": " + mean);

            return mean;
        }

        private async Task<double> GetAvgMeasureBySiteCodeRHDP(string startDate, string endDate, string siteCode, string typeOfMeasure)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);

            Query q = new Query();

            //Utilities.PrintParams(new string[4] { startDate, endDate, siteCode, typeOfMeasure });
            QueryDefinition queryDefinition = new QueryDefinition(q.averageSampleMeasurementByDateAndSiteCodeRHDP)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure)
                    .WithParameter("@siteCode", siteCode);
            var result = await this.RunQueryWithParams(rhContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double mean = 0.0;
            if(jsonResult.ContainsKey("average"))
            {
               mean = jsonResult["average"];
            }
            

            Console.WriteLine("Average/Mean "+typeOfMeasure+" Read for site " + siteCode + " on dates " + startDate + " - " + endDate + ": " + mean);

            return mean;
        }

    public string averageSampleMeasurementByDateAndSiteCode { get; } = @"
        SELECT AVG(c.sampleMeasurement) as average
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.siteCode = @siteCode)";

    public string averageSampleMeasurementByDateAndSiteCodeRHDP { get; } = @"
        SELECT AVG(c.sampleMeasurement) as average
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.siteCode = @siteCode) AND (c.parameterName = @typeOfMeasure)";