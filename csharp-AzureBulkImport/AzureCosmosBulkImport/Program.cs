using System;
using System.Threading.Tasks;
using System.Globalization;
using System.Configuration;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Net;
using Microsoft.Azure.Cosmos;
using ChoETL;

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
        private async Task CreateContainerAsync(string containerId, string idPath, Container container)
        {
            // Create a new containerId, Data.idPath
            ContainerProperties containerProperties = new ContainerProperties()
            {
                Id = containerId,
                PartitionKeyPath = idPath,
            };
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerProperties, ThroughputProperties.CreateAutoscaleThroughput(10000));
            Console.WriteLine("Created Container: {0}\n", this.container.Id);
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

        private async Task QueryForAverageMeasurementsAsync()
        {

            var sqlQueryText = "SELECT AVG(d.sampleMeasurement) FROM d";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<dynamic> queryResultSetIterator = this.container.GetItemQueryIterator<dynamic>(queryDefinition);

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
        }

        private async Task QueryForMaxMeasurementsAsync(Container container)
        {

            // var sqlQueryText = "SELECT DateTimePart('year', c.dateGMT) , AVG(c.sampleMeasurement) FROM c GROUP BY DateTimePart('year', c.dateGMT)";
            var sqlQueryText = "SELECT MIN(d.sampleMeasurement) FROM d";
            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<dynamic> queryResultSetIterator = this.container.GetItemQueryIterator<dynamic>(queryDefinition);

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
        }

        private async Task QueryForMinMeasurementsAsync()
        {

            var sqlQueryText = "SELECT MIN(d.sampleMeasurement) FROM d";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<dynamic> queryResultSetIterator = this.container.GetItemQueryIterator<dynamic>(queryDefinition);

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
        }

        private async Task<List<dynamic>> RunQuery(Container c, string sqlQueryText)
        {

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<dynamic> queryResultSetIterator = this.container.GetItemQueryIterator<dynamic>(queryDefinition);

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
            Console.WriteLine("Running query: {0}\n");

            FeedIterator<dynamic> queryResultSetIterator = this.container.GetItemQueryIterator<dynamic>(queryDefinition);

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
        private async Task DeleteDatabaseAndCleanupAsync()
        {
            DatabaseResponse databaseResourceResponse = await this.database.DeleteAsync();

            Console.WriteLine("Deleted Database: {0}\n", this.airquality_database);

            //Dispose of CosmosClient
            this.cosmosClient.Dispose();
        }

        

        public async Task GetStartedDemoAsync()
        {
            // Create a new instance of the Cosmos Client
            this.cosmosClient = new CosmosClient(EndpointUrl, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true });
            await this.CreateDatabaseAsync();
            await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);
            await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);

            //ONLY USE JSON OR CSV

            // Adding all rh dewpoint data JSON
            // await this.AddItemsToContainerAsyncJSON(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2017_json_small");
            // await this.AddItemsToContainerAsyncJSON(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2018_json_small");
            // await this.AddItemsToContainerAsyncJSON(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2019_json_small");
            // await this.AddItemsToContainerAsyncJSON(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2020_json_small");

            // Adding all pm data JSON
            // await this.AddItemsToContainerAsyncJSON(this.hourly_pm_container, "hourly_pm_data_2017_json_small");
            //await this.AddItemsToContainerAsyncJSON(this.hourly_pm_container, "hourly_pm_data_2018_json_small");
            //await this.AddItemsToContainerAsyncJSON(this.hourly_pm_container, "hourly_pm_data_2019_json_small");
            //await this.AddItemsToContainerAsyncJSON(this.hourly_pm_container, "hourly_pm_data_2020_json_small");


            // Adding all pm data CSV
            //await this.AddItemsToContainerAsyncCSV(this.hourly_pm_container, "hourly_pm_data_2017_csv");
            //await this.AddItemsToContainerAsyncCSV(this.hourly_pm_container, "hourly_pm_data_2018_csv");
            //await this.AddItemsToContainerAsyncCSV(this.hourly_pm_container, "hourly_pm_data_2019_csv");
            //await this.AddItemsToContainerAsyncCSV(this.hourly_pm_container, "hourly_pm_data_2020_csv");

            // Adding all rh dewpoint data CSV
            //await this.AddItemsToContainerAsyncCSV(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2017_csv");
            //await this.AddItemsToContainerAsyncCSV(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2018_csv");
            //await this.AddItemsToContainerAsyncCSV(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2019_csv");
            //await this.AddItemsToContainerAsyncCSV(this.hourly_rh_dewpoint_container, "hourly_rh_dp_data_2020_csv");

            // await this.AddItemsToContainerAsyncCSVStandard(this.hourly_pm_container, "hourly_pm_data_2017_csv");

            //Test Queries 
            //await this.QueryForAverageMeasurementsAsync();
            //await this.QueryForMaxMeasurementsAsync(this.hourly_rh_dewpoint_container_azure);
            //await this.QueryForMinMeasurementsAsync();

            //Easy Queries
            Query q = new Query();
            //await this.RunQuery(this.hourly_rh_dewpoint_container_azure, q.MinMaxBySite);
            //await this.RunQuery(q.AnnualPMAverage);
            //await this.RunQuery(q.PrePostCovidDifference);
            //await this.RunQuery(this.hourly_rh_dewpoint_container_azure, q.UndefinedOrNullData);
            //await this.RunQuery(this.hourly_rh_dewpoint_container_azure, q.NegativeMeasurements);
            //await this.RunQuery(this.hourly_rh_dewpoint_container_azure, q.RepeatedMeasurements);
            //await this.RunQuery(this.hourly_rh_dewpoint_container_azure, q.minSampleMeasurement);

            //await this.RunQuery(this.hourly_rh_dewpoint_container_azure, q.MedianSampleMeasurement);
            await this.GetMedianPerYearQuerySet("2020-01-01", "2020-12-31");


            // await this.DeleteDatabaseAndCleanupAsync();

            // this.LoadCSV();
        }

        public async Task GetMedianPerYearQuerySet(string startDate, string endDate)
        {
            this.cosmosClient = new CosmosClient(EndpointUrl, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true });
            await this.CreateDatabaseAsync();
            await this.CreateContainerAsync(this.hourly_pm_container, Data.idPath, hourly_pm_container_azure);
            await this.CreateContainerAsync(this.hourly_rh_dewpoint_container, Data.idPath, this.hourly_rh_dewpoint_container_azure);

            Query q = new Query();

            //string the queries together
            //first, get the # of "rows"/objects for the TOP command)
            Console.WriteLine(startDate);
            Console.WriteLine(endDate);
            QueryDefinition queryDefinition = new QueryDefinition(q.totalEntryCountByDate)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
            var entryMidpoint = (await this.RunQueryWithParams(this.hourly_rh_dewpoint_container_azure, queryDefinition));
            var jsonResult = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(entryMidpoint[0].ToString());
            Console.WriteLine("total number of entries for " + startDate + " , ending " + endDate + ": " + jsonResult);
            //for funsies
            Console.WriteLine("without above zero limitation ");
            QueryDefinition tempQuery = new QueryDefinition(q.totalEntryCountByDateNoAboveZero)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
            var entryMidpoint1 = (await this.RunQueryWithParams(this.hourly_rh_dewpoint_container_azure, tempQuery));
            var jsonResult1 = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(entryMidpoint1[0].ToString());
            Console.WriteLine("total number of entries for " + startDate + " , ending " + endDate + ": " + jsonResult1);

            if (jsonResult["$1"]%2 == 0) //even # of objects
            {
                var midPoint1 = (jsonResult["$1"] / 2) + 1;
                var midPoint2 = (jsonResult["$1"] / 2) - 1;

                queryDefinition = new QueryDefinition(q.getSimpleMedian)
                    .WithParameter("@rowNum", midPoint1)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result1 = await this.RunQueryWithParams(this.hourly_rh_dewpoint_container_azure, queryDefinition);

                queryDefinition = new QueryDefinition(q.getMedianLowerValue)
                    .WithParameter("@rowNum", midPoint2)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
                var result2 = await this.RunQueryWithParams(this.hourly_rh_dewpoint_container_azure, queryDefinition);

                string stResult1 = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result1[0].ToString());
                string stResult2 = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(result2[0].ToString());
                Console.WriteLine(stResult1);
                Console.WriteLine(stResult2);



            } else // odd # of objects
            {
                var midPoint = jsonResult["$1"] / 2 + 1; //the middle object, for reasons i dont understand it rounds down
                Console.WriteLine("midpoint in else clause: " + midPoint);

                //simplified since we dont need to average 2 numbers
                
               queryDefinition = new QueryDefinition(q.getSimpleMedian)
                    .WithParameter("@rowNum", midPoint)
                    .WithParameter("@startDate", startDate)
                    .WithParameter("@endDate", endDate);
               var result = await this.RunQueryWithParams(this.hourly_rh_dewpoint_container_azure, queryDefinition);
                Console.WriteLine(result);

            }
            
            
            
        }
    }
}
