using System;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Generic;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using System.IO;
using System.Linq;

namespace CosmosGettingStartedTutorial
{
    class Program
    {
        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string EndpointUri = ConfigurationManager.AppSettings["EndPointUri"];

        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = ConfigurationManager.AppSettings["PrimaryKey"];

        // The Cosmos client instance
        private CosmosClient cosmosClient;

        // The database we will create
        private Database database;

        // The container we will create.
        private Container container;

        // The name of the database and container we will create
        private readonly string databaseId = ConfigurationManager.AppSettings["databaseId"];
        private readonly string containerId = ConfigurationManager.AppSettings["containerId"];
        private readonly int AmountToInsert = int.Parse(ConfigurationManager.AppSettings["AmountToInsert"]);
        private readonly string partitionKey = ConfigurationManager.AppSettings["partitionKey"];

        // <Main>
        public static async Task Main(string[] args)
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
        // </Main>

        // <GetStartedDemoAsync>
        /// <summary>
        /// Entry point to call methods that operate on Azure Cosmos DB resources in this sample
        /// </summary>
        public async Task GetStartedDemoAsync()
        {
            // Create a new instance of the Cosmos Client
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions() { ApplicationName = "CosmosDBDotnetQuickstart" });
            await this.CreateDatabaseAsync();
            await this.CreateContainerAsync();
            await this.ScaleContainerAsync();

            // FIXME: was this supposed to be the code at the bottom?
            await this.AddItemsToContainerAsync();

            // FIXME: these are not defined/implemented
            //await this.QueryItemsAsync();
            //await this.ReplaceFamilyItemAsync();
            //await this.DeleteFamilyItemAsync();
            //await this.DeleteDatabaseAndCleanupAsync();
        }
        // </GetStartedDemoAsync>

        // <CreateDatabaseAsync>
        /// <summary>
        /// Create the database if it does not exist
        /// </summary>
        private async Task CreateDatabaseAsync()
        {
            // Create a new database
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine("Created Database: {0}\n", this.database.Id);
        }
        // </CreateDatabaseAsync>

        // <CreateContainerAsync>
        /// <summary>
        /// Create the container if it does not exist. 
        /// Specifiy "/timeLocal" as the partition key
        /// </summary>
        /// <returns></returns>
        private async Task CreateContainerAsync()
        {
            // Create a new container
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, Data.idPath, Convert.ToInt32(ConfigurationManager.AppSettings["throughput"]));
            Console.WriteLine("Created Container: {0}\n", this.container.Id);
        }
        // </CreateContainerAsync>

        // <ScaleContainerAsync>
        /// <summary>
        /// Scale the throughput provisioned on an existing Container.
        /// You can scale the throughput (RU/s) of your container up and down to meet the needs of the workload. Learn more: https://aka.ms/cosmos-request-units
        /// </summary>
        /// <returns></returns>
        private async Task ScaleContainerAsync()
        {
            // Read the current throughput
            int? throughput = await this.container.ReadThroughputAsync();
            if (throughput.HasValue)
            {
                Console.WriteLine("Current provisioned throughput : {0}\n", throughput.Value);
                int newThroughput = throughput.Value + 100;
                // Update throughput
                await this.container.ReplaceThroughputAsync(newThroughput);
                Console.WriteLine("New provisioned throughput : {0}\n", newThroughput);
            }
            
        }
        // </ScaleContainerAsync>

        // <LoadJson>
        public List<Data> LoadJson()
        {
            using (StreamReader r = new StreamReader(ConfigurationManager.AppSettings["jsonFile"]))
            {
                string json = r.ReadToEnd();
                return JsonConvert.DeserializeObject<List<Data>>(json);
            }
        }
        // </LoadJson>

        // <Data>
        public class Data
        {
            public const string idPath = "/id";

            [JsonProperty(PropertyName = "id")] // cosmos requires an "id" property, so just make "Id" identify as "id"
            public string Id {get;set;}

            public int stateCode { get; set; }
            public int countyCode { get; set; }
            public string siteNum { get; set; }
            public int poc { get; set; }
            public float latitude { get; set; }
            public float longitude { get; set; }
            public string datum { get; set; }
            public string parameterName { get; set; }
            public DateTime dateLocal { get; set; }
            public DateTime timeLocal { get; set; }
            public DateTime dateGMT { get; set; }
            public DateTime timeGMT { get; set; }
            public float sampleMeasurement { get; set; }
            public string unitOfMeasure { get; set; }
            public float mdl { get; set; }
            public string uncertainty { get; set; }
            public string qualifier { get; set; }
            public DateTime dateLastChange { get; set; }
        }
        // </Data>

        // FIXME: should the code that was here be in a method, or somewhere else???
        public async Task AddItemsToContainerAsync()
        {
            // FIXME: is GetContainer(containerId) correct? It was GetContainer(ContainerName) before...
            Container container = database.GetContainer(containerId);
            List<Task> tasks = new List<Task>(AmountToInsert);
            List<Data> dataToInsert = LoadJson();
            foreach (Data data in dataToInsert)
            {
                // FIXME: is data.timeGMT meant to be the partition key?
                // Should it be data.timeGMT.ToString() for PartitionKey.ctor(string)?
                // partitionKey was added above from the App.Config
                tasks.Add(container
                    .CreateItemAsync(data, new PartitionKey(data.Id))
                    .ContinueWith(taskWithItemResponse =>
                    {
                        // FIXME: verify this is what you want, IsCompletedSuccessfully is a .net core property, which does not seem to be in .net framework
                        if (taskWithItemResponse.Status != TaskStatus.RanToCompletion)
                        {
                            AggregateException innerExceptions = taskWithItemResponse.Exception.Flatten();
                            if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                            {
                                Console.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message}).\n");
                            }
                            else
                            {
                                Console.WriteLine($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.\n");
                            }
                        }
                    })
                );
            }

            // Wait until all are done
            await Task.WhenAll(tasks);
        }
    }
}
