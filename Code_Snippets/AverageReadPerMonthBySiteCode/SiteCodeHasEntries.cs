        private async Task<bool> SiteCodeHasEntries(string startDate, string endDate, string siteCode)
        {
            long pm, rh, dp = 0;
            pm = await this.GetNumSiteCodeEntriesPM(startDate, endDate, siteCode);
            rh = await this.GetNumSiteCodeEntriesRHDP(startDate, endDate, siteCode, "Relative Humidity");
            dp = await this.GetNumSiteCodeEntriesRHDP(startDate, endDate, siteCode, "Dew Point");

            Console.WriteLine(""+pm, rh, dp);
            if (pm > 0 || rh > 0 || dp > 0) {  return true; }
            return false;
        }

                private async Task<long> GetNumSiteCodeEntriesRHDP(string startDate, string endDate, string siteCode, string typeOfMeasure)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[4] { startDate, endDate, siteCode, typeOfMeasure });
            QueryDefinition queryDefinition = new QueryDefinition(q.totalEntryCountByDateAndSiteCodeRHDP)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@siteCode", siteCode)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
            var result = await this.RunQueryWithParams(rhContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            long count = jsonResult["count"];
            
            return count;
        }

        private async Task<long> GetNumSiteCodeEntriesPM(string startDate, string endDate, string siteCode)
        {
            Container pmContainer = await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[3] { startDate, endDate, siteCode });
            QueryDefinition queryDefinition = new QueryDefinition(q.totalEntryCountByDateAndSiteCodePM)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@siteCode", siteCode);
            var result = await this.RunQueryWithParams(pmContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            long count = jsonResult["count"];
            
            return count;
        }

    public string totalEntryCountByDateAndSiteCodePM { get; } = @"
        SELECT COUNT(c.id) as count
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.siteCode = @siteCode)";

    public string totalEntryCountByDateAndSiteCodeRHDP { get; } = @"
        SELECT COUNT(c.id) as count
        FROM c
        WHERE c.sampleMeasurement >= 0.0 AND (c.dateGMT BETWEEN @startDate AND @endDate) AND (c.parameterName = @typeOfMeasure) AND (c.siteCode = @siteCode)";