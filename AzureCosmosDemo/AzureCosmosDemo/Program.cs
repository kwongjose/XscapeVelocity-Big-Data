using System;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Net;
using Microsoft.Azure.Cosmos;

namespace todo
{
    public class Program
    {

        /// The Azure Cosmos DB endpoint for running this GetStarted sample.
        private string EndpointUrl = "https://localhost:8081";

        /// The primary key for the Azure DocumentDB account.
        private string PrimaryKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        // The Cosmos client instance
        private CosmosClient cosmosClient;

        // The database we will create
        private Database database;

        // The container we will create.
        private Container container;

        // The name of the database and container we will create
        private string databaseId = "DemoDB";
        private string containerId = "DBContainer";

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

        private async Task CreateDatabaseAsync()
        {
            // Create a new database
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine("Created Database: {0}\n", this.database.Id);
        }

        /// Create the container if it does not exist. 
        /// Specifiy "/LastName" as the partition key since we're storing family information, to ensure good distribution of requests and storage.
        private async Task CreateContainerAsync()
        {
            // Create a new container
            this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, "/siteNum");
            Console.WriteLine("Created Container: {0}\n", this.container.Id);
        }

        // <LoadJson>
        public List<Data> LoadJson()
        {
            using (StreamReader r = new StreamReader("../../../data.json"))
            {
                string json = r.ReadToEnd();
                Console.WriteLine(json);
                return JsonConvert.DeserializeObject<List<Data>>(json);
            }
        }
        // </LoadJson>

        private async Task AddItemsToContainerAsync()
        {
            List<Task> tasks = new List<Task>(10);
            Container container = database.GetContainer(containerId);
            List<Data> dataToInsert = LoadJson();

            Console.WriteLine(dataToInsert);

            try
            {
                // Create an item in the container representing the Andersen family. Note we provide the value of the partition key for this item, which is "Andersen".
                foreach (Data data in dataToInsert)
                {
                    tasks.Add(container
                    .CreateItemAsync(data, new PartitionKey(data.siteNum))
                    );
                    Console.WriteLine(data);
                    // ItemResponse<Data> dataResponse = await this.container.CreateItemAsync<Data>(data, new PartitionKey(data.siteNum));
                    // Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n", dataResponse.Resource.Id, dataResponse.RequestCharge);
                }
                await Task.WhenAll(tasks);
                // ItemResponse<Family> andersenFamilyResponse = await this.container.CreateItemAsync<Family>(andersenFamily, new PartitionKey(andersenFamily.LastName));
                // Note that after creating the item, we can access the body of the item with the Resource property of the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                // Console.WriteLine("Created item in database with id: {0} Operation consumed {1} RUs.\n", andersenFamilyResponse.Resource.Id, andersenFamilyResponse.RequestCharge);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
               // Console.WriteLine("Item in database with id: {0} already exists\n", andersenFamily.Id);
            }
        }

        private async Task QueryItemsAsync()
        {
            var sqlQueryText = "SELECT * FROM c WHERE c.LastName = 'Andersen'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);
            FeedIterator<Family> queryResultSetIterator = this.container.GetItemQueryIterator<Family>(queryDefinition);

            List<Family> families = new List<Family>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Family> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (Family family in currentResultSet)
                {
                    families.Add(family);
                    Console.WriteLine("\tRead {0}\n", family);
                }
            }
        }

        private async Task DeleteDatabaseAndCleanupAsync()
        {
            DatabaseResponse databaseResourceResponse = await this.database.DeleteAsync();
            // Also valid: await this.cosmosClient.Databases["FamilyDatabase"].DeleteAsync();

            Console.WriteLine("Deleted Database: {0}\n", this.databaseId);

            //Dispose of CosmosClient
            this.cosmosClient.Dispose();
        }

        public async Task GetStartedDemoAsync()
        {
            // Create a new instance of the Cosmos Client
            this.cosmosClient = new CosmosClient(EndpointUrl, PrimaryKey, new CosmosClientOptions() { AllowBulkExecution = true });
            await this.CreateDatabaseAsync();
            await this.CreateContainerAsync();
            await this.AddItemsToContainerAsync();
            // await this.QueryItemsAsync();
            // await this.DeleteDatabaseAndCleanupAsync();
        }
    }
}
