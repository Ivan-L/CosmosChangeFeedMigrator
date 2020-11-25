namespace CosmosChangeFeedMigrator
{
    public class CosmosConfiguration
    {
        public string AccountEndpoint { get; set; }

        public string AuthKey { get; set; }

        public string DatabaseName { get; set; }
        
        public string LeaseContainerName { get; set; }
        
        public int? LeaseContainerThroughput { get; set; }
    }
}