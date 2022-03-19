        public async Task GetNumOfInvalidMeasures()
        {
            await this.InvalidPM();
            await this.InvalidRHDP("Relative Humidity");
            await this.InvalidRHDP("Dew Point");
        }

        private async Task InvalidPM() 
        {
            Container pmContainer = await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[1] { "" + 0.0 });
            QueryDefinition queryDefinition = new QueryDefinition(q.countOccurencesOfMeasurementLessThan)
                    .WithParameter("@measure", 0.0);
            var result = await this.RunQueryWithParams(pmContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double occ = jsonResult["numOfOccurences"];

            Console.WriteLine("Number of invalid PM2.5 Reads: " + occ);
        }

        private async Task InvalidRHDP(string type)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);
            Query q = new Query();
            Utilities.PrintParams(new string[2] { "" + 0.0, type });
            QueryDefinition queryDefinition = new QueryDefinition(q.countOccurencesOfMeasurementLessThan)
                    .WithParameter("@measure", 0.0)
                    .WithParameter("@typeOfMeasure", type);
            var result = await this.RunQueryWithParams(rhContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double occ = jsonResult["numOfOccurences"];

            Console.WriteLine("Number of invalid "+type+" Reads: " + occ);
        }

    public string countOccurencesOfMeasurementLessThan { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement < @measure";

    public string countOccurencesOfMeasurementLessThanRHDP { get; } = @"
        SELECT COUNT(c.sampleMeasurement) as numOfOccurences
        FROM c
        WHERE c.sampleMeasurement < @measure AND (c.parameterName = @typeOfMeasure)";