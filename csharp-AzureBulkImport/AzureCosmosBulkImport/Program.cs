using System;
using System.Threading.Tasks;
using System.Globalization;
using System.Configuration;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Net;
using Microsoft.Azure.Cosmos;
using ChoETL;
using System.Text;

namespace AzureBulkImport
{
    public class Program
    {

        /// The Azure Cosmos DB endpoint for running this GetStarted sample.
        private string EndpointUrl = ConfigurationManager.AppSettings.Get("EndpointUri");

        /// The primary key for the Azure DocumentDB account.
        private string PrimaryKey = ConfigurationManager.AppSettings.Get("PrimaryKey");

        // The Cosmos client instance
        private CosmosClient cosmosClient;

        // The database we will create
        private Database database;

        // The container we will create.
        private Container container;

        // The container we will create.
        private Container hourly_pm_container_azure;

        // The container we will create.
        private Container hourly_rh_dewpoint_container_azure;

        //TODO: delete
        private Container dummy_data;

        private string airquality_database = "airquality";
        private string hourly_rh_dewpoint_container = "hourly-rh-dewpoint";
        private string hourly_pm_container = "hourly-pm";

        public static async Task Main(string[] args)
        {
            if (args.Length == 0 | args == null)
            {
                Console.WriteLine("args is null");
            }
            else
            {
                try
                {
                    Console.WriteLine("Beginning operations...\n");
                    Program p = new Program();
                    await p.GetStartedDemoAsync();

                }
                catch (CosmosException de)
                {
                    Exception baseException = de.GetBaseException();
                    Console.WriteLine("{0} error occurred: {1}", de.StatusCode, de);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error: {0}", e);
                }
                finally
                {
                    Console.WriteLine("End of demo, press any key to exit.");
                    Console.ReadKey();
                }
            }
            
        }

        private async Task CreateDatabaseAsync()
        {
            // Create a new database
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(airquality_database);
            Console.WriteLine("Created Database: {0}\n", this.database.Id);
        }

        /// Create the container if it does not exist. 
        /// Specifiy "/idPath" as the partition key
        private async Task<Container> CreateContainerAsync(string containerId, string idPath, Container container)
        {
            // Create a new containerId, Data.idPath
            ContainerProperties containerProperties = new ContainerProperties()
            {
                Id = containerId,
                PartitionKeyPath = idPath,
            };
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerProperties, ThroughputProperties.CreateAutoscaleThroughput(10000));
            Console.WriteLine("Created Container: {0}\n", this.container.Id);

            return this.container;
        }

        private async Task AddItemsToContainerAsyncJSON(string containerId, string jsonFile)
        {
            List<Task> tasks = new List<Task>();
            Container container = database.GetContainer(containerId);
            foreach (var data in new ChoJSONReader<Data>(ConfigurationManager.AppSettings.Get(jsonFile)))
            {
                tasks.Add(container
                    .CreateItemAsync(data, new PartitionKey(data.Id))
                    .ContinueWith(itemResponse =>
                    {
                        if (!itemResponse.IsCompletedSuccessfully)
                        {
                            AggregateException innerExceptions = itemResponse.Exception.Flatten();
                            if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                            {
                                Console.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                            }
                            else
                            {
                                Console.WriteLine($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                            }
                        }
                    })
                    );

            }
            await Task.WhenAll(tasks);
        }

        private async Task AddItemsToContainerAsyncCSV(string containerId, string csvFile)
        {
            List<Task> tasks = new List<Task>();
            Container container = database.GetContainer(containerId);
            string testDate = "2017-01-10";
            DateTime exampleDateTime = DateTime.Parse(testDate);
            foreach (var dataObject in new ChoCSVReader(ConfigurationManager.AppSettings.Get(csvFile)).WithFirstLineHeader())
            {
                Data data = new Data();
                location location = new location();
                location.type = "Point";
                location.coordinates = new float[] { float.Parse(dataObject.Longitude), float.Parse(dataObject.Latitude) };
                data.Id = System.Guid.NewGuid().ToString();
                data.siteCode = dataObject.StateCode.Replace("\"", "") + dataObject.CountyCode.Replace("\"", "") + dataObject.SiteNum.Replace("\"", "");
                data.location = location;
                data.poc = int.Parse(dataObject.POC);
                data.datum = dataObject.Datum.Replace("\"", "");
                data.parameterName = dataObject.ParameterName.Replace("\"", "");
                data.dateLocal = DateTime.ParseExact(dataObject.DateLocal.Replace("\"", ""), "yyyy-MM-dd", null);
                data.timeLocal = DateTime.Parse(dataObject.TimeLocal.Replace("\"", ""));
                data.dateGMT = DateTime.Parse(dataObject.DateGMT.Replace("\"", ""));
                data.timeGMT = DateTime.Parse(dataObject.TimeGMT.Replace("\"", ""));
                data.sampleMeasurement = float.Parse(dataObject.SampleMeasurement);
                data.unitOfMeasure = dataObject.UnitsOfMeasure.Replace("\"", "");
                data.mdl = float.Parse(dataObject.MDL);
                data.uncertainty = dataObject.Uncertainty.Replace("\"", "");
                data.qualifier = dataObject.Qualifier.Replace("\"", "");
                data.dateLastChange = DateTime.Parse(dataObject.DateOfLastChange.Replace("\"", ""));
                tasks.Add(container
                    .CreateItemAsync(data, new PartitionKey(data.Id))
                    .ContinueWith(itemResponse =>
                    {
                        if (!itemResponse.IsCompletedSuccessfully)
                        {
                            AggregateException innerExceptions = itemResponse.Exception.Flatten();
                            if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                            {
                                Console.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                            }
                            else
                            {
                                Console.WriteLine($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                            }
                        }
                    })
                );
            }
            await Task.WhenAll(tasks);
        }

        private async Task AddItemsToContainerAsyncCSVStandard(string containerId, string csvFile)
        {
            List<Task> tasks = new List<Task>();
            Container container = database.GetContainer(containerId);
            foreach (var dataObject in new ChoCSVReader(ConfigurationManager.AppSettings.Get(csvFile)).WithFirstLineHeader())
            {
                Data data = new Data();
                location location = new location();
                location.type = "Point";
                location.coordinates = new float[] { float.Parse(dataObject.Longitude), float.Parse(dataObject.Latitude) };
                data.Id = System.Guid.NewGuid().ToString();
                data.siteCode = dataObject.StateCode.Replace("\"", "") + dataObject.CountyCode.Replace("\"", "") + dataObject.SiteNum.Replace("\"", "");
                data.location = location;
                data.poc = int.Parse(dataObject.POC);
                data.datum = dataObject.Datum.Replace("\"", "");
                data.parameterName = dataObject.ParameterName.Replace("\"", "");
                data.dateLocal = DateTime.ParseExact(dataObject.DateLocal.Replace("\"", ""), "yyyy-MM-dd", null);
                data.timeLocal = DateTime.Parse(dataObject.TimeLocal.Replace("\"", ""));
                data.dateGMT = DateTime.Parse(dataObject.DateGMT.Replace("\"", ""));
                data.timeGMT = DateTime.Parse(dataObject.TimeGMT.Replace("\"", ""));
                data.sampleMeasurement = float.Parse(dataObject.SampleMeasurement);
                data.unitOfMeasure = dataObject.UnitsOfMeasure.Replace("\"", "");
                data.mdl = float.Parse(dataObject.MDL);
                data.uncertainty = dataObject.Uncertainty.Replace("\"", "");
                data.qualifier = dataObject.Qualifier.Replace("\"", "");
                data.dateLastChange = DateTime.Parse(dataObject.DateOfLastChange.Replace("\"", ""));
                var response = await container.CreateItemAsync(data, new PartitionKey(data.Id));
            }            
        }

        private async Task<List<dynamic>> RunQuery(Container c, string sqlQueryText)
        {

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<dynamic> queryResultSetIterator = c.GetItemQueryIterator<dynamic>(queryDefinition);

            List<dynamic> allData = new List<dynamic>();
            Console.WriteLine("\tQuery Iterator {0}\n", queryResultSetIterator);

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<dynamic> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                Console.WriteLine("\tResults {0}\n", currentResultSet);
                foreach (dynamic data in currentResultSet)
                {
                    allData.Add(data);
                    Console.WriteLine("\tRead {0}\n", data);
                }
            }
            return allData;
        }

        private async Task<List<dynamic>> RunQueryWithParams(Container c, QueryDefinition queryDefinition)
        {
            //Console.WriteLine("Running query: {0}\n", queryDefinition.QueryText);

            FeedIterator<dynamic> queryResultSetIterator = c.GetItemQueryIterator<dynamic>(queryDefinition);

            List<dynamic> allData = new List<dynamic>();


            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<dynamic> currentResultSet = await queryResultSetIterator.ReadNextAsync();

                foreach (dynamic data in currentResultSet)
                {
                    allData.Add(data);
                }
            }
            return allData;
        }
        private async Task DeleteDatabaseAndCleanupAsync()
        {
            DatabaseResponse databaseResourceResponse = await this.database.DeleteAsync();

            Console.WriteLine("Deleted Database: {0}\n", this.airquality_database);

            //Dispose of CosmosClient
            this.cosmosClient.Dispose();
        }

        

        public async Task GetStartedDemoAsync()
        {
            /********************/
            // Create Cosmos Client
            /********************/
            this.cosmosClient = new CosmosClient(EndpointUrl, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true });
            await this.CreateDatabaseAsync();
            await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);
            await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);


            /********************/
            // Load Data to DB
            /********************/

            //ONLY USE JSON OR CSV
            //await this.AddJSONData();
            //await this.AddCSVData();


            /********************/
            // Initialize Values
            /********************/

            Constants constant = new Constants();
            var sites = constant.LoadSitesFromCSV();
            Query q = new Query();


            /********************/
            // Easy Queries
            /********************/

            //Median, Min, and Max Measurements Per Year
            for (int i = 0; i < constant.monthlyDateArray.Length; i++)
            {
                // Get Median PM2.5 Data Per Year
                await this.GetMedianPerYearQuerySet(constant.monthlyDateArray[i], constant.monthlyDateArray[i + 1]);
                await this.GetMinMeasurePerYearQuerySet(constant.monthlyDateArray[i], constant.monthlyDateArray[i + 1]);
                await this.GetMaxMeasurePerYearQuerySet(constant.monthlyDateArray[i], constant.monthlyDateArray[i + 1]);
                //skip 1 because every other is an end date
                i++;
            }


            //Pre-Post Covid Median Measurements
            //await this.GetDifferenceSinceCovid();

            //Invalid Measurements
            //await this.GetNumOfInvalidMeasures();

            //Get Monthly Mean per SiteCode and Save to CSV
            await this.GetMonthlyAvgBySiteCode(sites, constant.monthlyDateArray);

            /********************/
            // Medium Queries
            /********************/

            //Find Outlier Information via IQR
            //first, find Q1 and Q3
            //then calculate for IQR
            //then calculate upper bound = Q3 + (1.5 * IQR)
            //then calculate your lower bound = Q1 – (1.5 * IQR)
            //finally, query for any sample measurements above upper bound and below lower bound


            /********************/
            // Database Cleanup
            /********************/
            // await this.DeleteDatabaseAndCleanupAsync();

        }

        public async Task<double[]> GetMedianPerYearQuerySet(string startDate, string endDate)
        {
            double median = await this.GetMedianPerYearPM2(startDate, endDate);
            double rhmedian = await this.GetMedianPerYearRHDP(startDate, endDate, "Relative Humidity");
            double dpmedian = await this.GetMedianPerYearRHDP(startDate, endDate, "Dew Point");

            double[] medianArray = new double[] { median, rhmedian, dpmedian };

            return medianArray;
        }

        public async Task<double> GetLowerQuartileByDateQuerySet(string startDate, string endDate)
        {
            return 0.0;
        }

        public async Task<double> GetUpperQuartileByDateQuerySet(string startDate, string endDate)
        {
            return 0.0;
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
            if ((jsonResult["$1"]/4) % 2 == 0) //even # of objects
            {

                Console.WriteLine("************* EVEN # OBJECTS *********\n");

                double lq1 = (((1 / 4) * jsonResult["$1"]) + 1);
                double lq2 = ((3 / 4) * (jsonResult["$1"] + 1)); //the middle objects, for reasons i dont understand this rounds down, so correcting....

                Utilities.PrintParams(new string[3] { startDate, endDate, "" + lq1 });
                queryDefinition = new QueryDefinition(q.getMedianUpperValue)
                    .WithParameter("@rowNum", lq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result1 = await this.RunQueryWithParams(pmContainer, queryDefinition);


                Utilities.PrintParams(new string[3] { startDate, endDate, "" + lq2 });
                queryDefinition = new QueryDefinition(q.getMedianLowerValue)
                    .WithParameter("@rowNum", lq2)
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

                lq = (md1 + md2) / 2;
                Console.WriteLine("Lower Quartile PM2.5 Read for dates " + startDate + " - " + endDate + ": " + lq);

            }
            else // odd # of objects
            {

                Console.WriteLine("********* ODD # OBJECTS ************\n");

                var lq1 = ((1 / 4) * jsonResult["$1"] + 1); //the middle object, for reasons i dont understand it rounds down

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

            //then, find midpoints and calculate
            if ((jsonResult["$1"] /4) % 2 == 0) //even # of objects
            {

                Console.WriteLine("************* EVEN # OBJECTS *********\n");
                
                double uq1 = (((3 / 4) * jsonResult["$1"]) + 1);
                double uq2 = ((1 / 4) * (jsonResult["$1"] + 1)); //the middle objects, for reasons i dont understand this rounds down, so correcting....

                Utilities.PrintParams(new string[3] { startDate, endDate, "" + uq1 });
                queryDefinition = new QueryDefinition(q.getMedianUpperValue)
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
                double md1 = FindHighestValue(result1);
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

                var uq1 = (jsonResult["$1"] + 1) / 4; //the middle object, for reasons i dont understand it rounds down

                //simplified since we dont need to average 2 numbers
                Utilities.PrintParams(new string[3] { startDate, endDate, "" + uq1 });
                queryDefinition = new QueryDefinition(q.getMedianUpperValue)
                    .WithParameter("@rowNum", uq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result = await this.RunQueryWithParams(pmContainer, queryDefinition);


                //finds the highest value of this subset of data. MAX of ASC data
                //because we only need the one, no additional arithmetic is needed.
                uq = FindHighestValue(result);

                Console.WriteLine("Upper Quartile PM2.5 Read for dates " + startDate + " - " + endDate + ": " + lq);
            }
            return uq;
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

            //then, find midpoints and calculate
            if ((jsonResult["$1"] / 4) % 2 == 0) //even # of objects
            {

                Console.WriteLine("************* EVEN # OBJECTS *********\n");

                double lq1 = (((1 / 4) * jsonResult["$1"]) + 1);
                double lq2 = ((3 / 4) * (jsonResult["$1"] + 1)); //the middle objects, for reasons i dont understand this rounds down, so correcting....

                Utilities.PrintParams(new string[4] { startDate, endDate, "" + lq1, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianUpperValueRHDP)
                    .WithParameter("@rowNum", lq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result1 = await this.RunQueryWithParams(rhContainer, queryDefinition);


                Utilities.PrintParams(new string[4] { startDate, endDate, "" + lq2, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianLowerValueRHDP)
                    .WithParameter("@rowNum", lq2)
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

                lq = (md1 + md2) / 2;
                Console.WriteLine("Lower Quartile PM2.5 Read for dates " + startDate + " - " + endDate + ": " + lq);

            }
            else // odd # of objects
            {

                Console.WriteLine("********* ODD # OBJECTS ************\n");

                var lq1 = ((1 / 4) * jsonResult["$1"] + 1); //the middle object, for reasons i dont understand it rounds down

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

                Console.WriteLine("Lower Quartile PM2.5 Read for dates " + startDate + " - " + endDate + ": " + lq);
            }
            return lq;
        }

        private async Task<double> GetUpperQuartileByDateRHDP(string startDate, string endDate, string typeOfMeasure)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);


            Query q = new Query();
            double uq = 0.0;

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
            if ((jsonResult["$1"] / 4) % 2 == 0) //even # of objects
            {

                Console.WriteLine("************* EVEN # OBJECTS *********\n");

                double uq1 = (((3 / 4) * jsonResult["$1"]) + 1);
                double uq2 = ((1 / 4) * (jsonResult["$1"] + 1)); //the middle objects, for reasons i dont understand this rounds down, so correcting....

                Utilities.PrintParams(new string[4] { startDate, endDate, "" + uq1, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianUpperValueRHDP)
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
                double md1 = FindHighestValue(result1);
                //finds the lowest value of this subset of data. MIN of DESC data
                double md2 = FindLowestValue(result2);

                Console.WriteLine("median1: " + md1);
                Console.WriteLine("median2: " + md2);

                uq = (md1 + md2) / 2;
                Console.WriteLine("Upper Quartile "+typeOfMeasure+".5 Read for dates " + startDate + " - " + endDate + ": " + uq);

            }
            else // odd # of objects
            {

                Console.WriteLine("********* ODD # OBJECTS ************\n");

                var uq1 = (jsonResult["$1"] + 1) / 4; //the middle object, for reasons i dont understand it rounds down

                //simplified since we dont need to average 2 numbers
                Utilities.PrintParams(new string[4] { startDate, endDate, "" + uq1, typeOfMeasure });
                queryDefinition = new QueryDefinition(q.getMedianUpperValueRHDP)
                    .WithParameter("@rowNum", uq1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
                var result = await this.RunQueryWithParams(rhContainer, queryDefinition);


                //finds the highest value of this subset of data. MAX of ASC data
                //because we only need the one, no additional arithmetic is needed.
                uq = FindHighestValue(result);

                Console.WriteLine("Upper Quartile PM2.5 Read for dates " + startDate + " - " + endDate + ": " + uq);
            }
            return uq;
        }


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
        
        public async Task<double[]> GetAverageMeasurePerYearQuerySet(string startDate, string endDate)
        {

            double mean = await this.GetAvgMeasurePM(startDate, endDate);
            double rhmean = await this.GetAvgMeasureRHDP(startDate, endDate, "Relative Humidity");
            double dpmean = await this.GetAvgMeasureRHDP(startDate, endDate, "Dew Point");

            double[] meanArray = new double[] { mean, rhmean, dpmean };

            return meanArray;
        }

        public async Task<double[]> GetAverageMeasurePerDateAndSiteQuerySet(string startDate, string endDate, string siteCode)
        {

            double mean = await this.GetAvgMeasureBySiteCodePM(startDate, endDate, siteCode);
            double rhmean = await this.GetAvgMeasureBySiteCodeRHDP(startDate, endDate, siteCode, "Relative Humidity");
            double dpmean = await this.GetAvgMeasureBySiteCodeRHDP(startDate, endDate, siteCode, "Dew Point");

            double[] meanArray = new double[] { mean, rhmean, dpmean };

            return meanArray;
        }

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

        private async Task<double> GetAvgMeasurePM(string startDate, string endDate)
        {
            Container pmContainer = await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[2] { startDate, endDate });
            QueryDefinition queryDefinition = new QueryDefinition(q.averageSampleMeasurementByDate)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
            var result = await this.RunQueryWithParams(pmContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double mean = jsonResult["average"];

            Console.WriteLine("Average/Mean PM2.5 Read for dates " + startDate + " - " + endDate + ": " + mean);

            return mean;
        }

        private async Task<double> GetAvgMeasureRHDP(string startDate, string endDate, string typeOfMeasure)
        {
            Container rhContainer = await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);

            Query q = new Query();

            Utilities.PrintParams(new string[3] { startDate, endDate, typeOfMeasure });
            QueryDefinition queryDefinition = new QueryDefinition(q.averageSampleMeasurementByDateRHDP)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate)
                    .WithParameter("@typeOfMeasure", typeOfMeasure);
            var result = await this.RunQueryWithParams(rhContainer, queryDefinition);
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result[0].ToString());
            double mean = jsonResult["average"];

            Console.WriteLine("Average/Mean "+typeOfMeasure+" Read for dates " + startDate + " - " + endDate + ": " + mean);

            return mean;
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

        public async Task GetDifferenceSinceCovid()
        {
            /*            double preCovidMedian = await this.GetMedianPerYearQuerySet("2017-01-01", "2020-03-01");
                        double postCovidMedian = await this.GetMedianPerYearQuerySet("2020-03-01", "2020-12-31");
                        double diffMedian = preCovidMedian - postCovidMedian;
                        Console.WriteLine("Pre-Covid Median PM2.5 Read is " + preCovidMedian);
                        Console.WriteLine("Post-Covid Median PM2.5 Read is " + postCovidMedian);
                        Console.WriteLine("The difference in PM2.5 Read data is " + diffMedian);
            */
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

        public async Task GetNumOfInvalidMeasures()
        {
            await this.InvalidPM();
            await this.InvalidRHDP("Relative Humidity");
            await this.InvalidRHDP("Dew Point");
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

        private double FindHighestValue(List<dynamic> result)
        {
            double highest_measure = Double.MinValue;
            foreach (var measure in result)
            {
                var obj = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(measure.ToString());
                if (obj["median"] >= highest_measure)
                {
                    highest_measure = obj["median"];

                }
            }//end of loop
            return highest_measure;
        }

        private double FindLowestValue(List<dynamic> result)
        {
            double lowest_measure = Double.MaxValue;
            foreach (var measure in result)
            {
                var obj = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(measure.ToString());
                
                if (obj["median"] <= lowest_measure)
                {
                    lowest_measure = obj["median"];

                }
            }//end of loop

            return lowest_measure;
        }

        private async Task<bool> SiteCodeHasEntries(string startDate, string endDate, string siteCode)
        {
            long pm, rh, dp = 0;
            pm = await this.GetNumSiteCodeEntriesPM(startDate, endDate, siteCode);
            rh = await this.GetNumSiteCodeEntriesRHDP(startDate, endDate, siteCode, "Relative Humidity");
            dp = await this.GetNumSiteCodeEntriesRHDP(startDate, endDate, siteCode, "Dew Point");

            Console.WriteLine(""+pm, rh, dp);
            if (pm > 0 || rh > 0 || dp > 0) { 
                //Console.WriteLine("true");
                return true; }
            //Console.WriteLine("false");
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

        public async Task AddJSONData()
        {
            await this.AddItemsToContainerAsyncJSON(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2017_json");
            await this.AddItemsToContainerAsyncJSON(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2018_json");
            await this.AddItemsToContainerAsyncJSON(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2019_json");
            await this.AddItemsToContainerAsyncJSON(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2020_json");

            //Adding all pm data JSON
            await this.AddItemsToContainerAsyncJSON(this.hourly_pm_container, "hourly_pm_data_2017_json_small");
            await this.AddItemsToContainerAsyncJSON(this.hourly_pm_container, "hourly_pm_data_2018_json_small");
            await this.AddItemsToContainerAsyncJSON(this.hourly_pm_container, "hourly_pm_data_2019_json_small");
            await this.AddItemsToContainerAsyncJSON(this.hourly_pm_container, "hourly_pm_data_2020_json_small");
        }

        public async Task AddCSVData()
        {
            // Adding all pm data CSV
            await this.AddItemsToContainerAsyncCSV(this.hourly_pm_container, "hourly_pm_data_2017_csv");
            await this.AddItemsToContainerAsyncCSV(this.hourly_pm_container, "hourly_pm_data_2018_csv");
            await this.AddItemsToContainerAsyncCSV(this.hourly_pm_container, "hourly_pm_data_2019_csv");
            await this.AddItemsToContainerAsyncCSV(this.hourly_pm_container, "hourly_pm_data_2020_csv");

            // Adding all rh dewpoint data CSV
            await this.AddItemsToContainerAsyncCSV(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2017_csv");
            await this.AddItemsToContainerAsyncCSV(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2018_csv");
            await this.AddItemsToContainerAsyncCSV(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2019_csv");
            await this.AddItemsToContainerAsyncCSV(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2020_csv");

            // await this.AddItemsToContainerAsyncCSVStandard(this.hourly_pm_container, "hourly_pm_data_2017_csv");
        }
    }
}