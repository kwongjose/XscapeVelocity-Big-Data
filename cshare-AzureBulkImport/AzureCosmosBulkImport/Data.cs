using Newtonsoft.Json;

namespace AzureBulkImport
{
    public class Data
    {
        [JsonProperty(PropertyName = "id")]
        public string? Id { get; set; }
        public string stateCode { get; set; }
        public string countyCode { get; set; }
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
        // The ToString() method is used to format the output, it's used for demo purpose only. It's not required by Azure Cosmos DB
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }

}