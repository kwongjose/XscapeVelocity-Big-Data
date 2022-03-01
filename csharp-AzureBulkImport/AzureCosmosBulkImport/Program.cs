using System;
using System.Threading.Tasks;
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

        // The name of the database and container we will create
        private string databaseId = "mock-airdata";
        private string containerId = "mock-hourly-dewpoint-data";

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
                    await p.GetStartedDemoAsync(args);

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
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine("Created Database: {0}\n", this.database.Id);
        }

        /// Create the container if it does not exist. 
        /// Specifiy "/idPath" as the partition key
        private async Task CreateContainerAsync()
        {
            // Create a new container
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, Data.idPath);
            Console.WriteLine("Created Container: {0}\n", this.container.Id);
        }

        // <LoadJson>
        public List<Data> LoadJson(string jsonFile)
        {
            using (StreamReader r = new StreamReader(ConfigurationManager.AppSettings.Get("jsonFile")))
            {
                string json = r.ReadToEnd();
                Console.WriteLine(json);
                return JsonConvert.DeserializeObject<List<Data>>(json);
                // return JsonConvert.DeserializeObject<List<Data>>(r, typeof(List<Data>), null); Something like this instead. 
            }
        }
        // </LoadJson>

        private async Task AddItemsToContainerAsync(string jsonFile)
        {
            List<Task> tasks = new List<Task>();
            Container container = database.GetContainer(containerId);
            // List<Data> dataToInsert = LoadJson(jsonFile);
            foreach (var data in new ChoJSONReader<Data>(ConfigurationManager.AppSettings.Get("jsonFile")))
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
            //try
            //{
            //    foreach (Data data in dataToInsert)
            //    {
            //        tasks.Add(container
            //        .CreateItemAsync(data, new PartitionKey(data.Id))
            //        .ContinueWith(itemResponse =>
            //        {
            //            if (!itemResponse.IsCompletedSuccessfully)
            //            {
            //                AggregateException innerExceptions = itemResponse.Exception.Flatten();
            //                if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
            //                {
            //                    Console.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
            //                }
            //                else
            //                {
            //                    Console.WriteLine($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
            //                }
            //            }
            //        })
            //        );
            //    }
            //    await Task.WhenAll(tasks);

            //}
            //catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            //{

            //}
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

        private async Task QueryForMaxMeasurementsAsync()
        {

            var sqlQueryText = "SELECT MAX(d.sampleMeasurement) FROM d";

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

        private async Task DeleteDatabaseAndCleanupAsync()
        {
            DatabaseResponse databaseResourceResponse = await this.database.DeleteAsync();

            Console.WriteLine("Deleted Database: {0}\n", this.databaseId);

            //Dispose of CosmosClient
            this.cosmosClient.Dispose();
        }

        public void LoadCSV()
        {
            //foreach (dynamic e in new ChoCSVReader(ConfigurationManager.AppSettings.Get("csvFile")).WithFirstLineHeader())
            //{
            //    Console.WriteLine(e.StateCode + " " + e.TimeGMT);
            //}
            foreach (var e in new ChoJSONReader<Data>(ConfigurationManager.AppSettings.Get("jsonFile")))
            {
                Console.WriteLine(e.ToString());
            }
                

        }

        public async Task GetStartedDemoAsync(string[] args)
        {
            // Create a new instance of the Cosmos Client
            this.cosmosClient = new CosmosClient(EndpointUrl, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true });
            await this.CreateDatabaseAsync();
            await this.CreateContainerAsync();
            await this.AddItemsToContainerAsync(args[0]);

            // Queries 
            //await this.QueryForAverageMeasurementsAsync();
            //await this.QueryForMaxMeasurementsAsync();
            //await this.QueryForMinMeasurementsAsync();

            // await this.DeleteDatabaseAndCleanupAsync();

            // this.LoadCSV();
        }
    }
}
