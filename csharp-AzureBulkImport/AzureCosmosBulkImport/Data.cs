using Newtonsoft.Json;
using ChoETL;


namespace AzureBulkImport
{
    [ChoCSVFileHeader]
    public class Data
    {
        [JsonProperty(PropertyName = "id")]
        [ChoIgnoreMember]
        public string? Id { get; set; } = System.Guid.NewGuid().ToString();

        public const string idPath = "/id";
        public string siteCode { get; set; }
        public int poc { get; set; }
        public location location { get; set; }
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
        // The ToString() method is used to format the output, it's used for demo purpose only. It's not required by Azure Cosmos DB
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }

    public class location
    {
        public string type { get; set; }
        public float[] coordinates { get; set; }
    }

}