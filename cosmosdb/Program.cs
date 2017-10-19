using System;

// ADD THIS PART TO YOUR CODE
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System.IO;

namespace cosmosdb
{
    class Program
    {
        // ADD THIS PART TO YOUR CODE
        private const string EndpointUri = "https://equityinsights.documents.azure.com:443/";
        private const string PrimaryKey = "txClUBsd1i2tQU5PXZXT6i642pJCVCyxDd32LOREJmZcHVUyT4DJFfBDLdnE2xcuzWeU6UozBNAAVce9F20jkw==";
        private DocumentClient client;
        // windows os
        string path = @"C:\code\cosmosdb_to_postgresql\insert.sh";
        // linux os
        // string path = @"/home/scpex/code/insert.sh";

        static void Main(string[] args)
        {
            // ADD THIS PART TO YOUR CODE
            try
            {
                Program p = new Program();
                p.GetStartedDemo().Wait();
            }
            catch (DocumentClientException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }
        // ADD THIS PART TO YOUR CODE
        private void WriteToConsoleAndPromptToContinue(string format, params object[] args)
        {
            Console.WriteLine(format, args);
            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
        }
        // ADD THIS PART TO YOUR CODE
        private async Task GetStartedDemo()
        {
             this.client = new DocumentClient(new Uri(EndpointUri), PrimaryKey);
            // ADD THIS PART TO YOUR CODE
            await this.client.CreateDatabaseIfNotExistsAsync(new Database { Id = "secforms" });
            //await this.client.CreateDocumentCollectionIfNotExistsAsync(UriFactory.CreateDatabaseUri("secforms"), new DocumentCollection { Id = "form4" });
            this.ExecuteSimpleQuery("secforms", "form4");
        }
        // ADD THIS PART TO YOUR CODE
        private void ExecuteSimpleQuery(string databaseName, string collectionName)
        {
            // Set some common query options
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = true };

            //int pageSize = 100000;
            //for (int i = 0; i < 100; i++)
            //{
            //            IQueryable<dynamic> familyQuery = this.client.CreateDocumentQuery<dynamic>(
            //    UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
            //    queryOptions).Take(pageSize).Skip(pageSize * i);
            //}



            //// Here we find the Andersen family via its LastName
            //IQueryable<dynamic> familyQuery = this.client.CreateDocumentQuery<dynamic>(
            //        UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
            //        queryOptions).Take(2);

            //// The query is executed synchronously here, but can also be executed asynchronously via the IDocumentQuery<T> interface
            //Console.WriteLine("Running LINQ query...");
            //foreach (dynamic family in familyQuery)
            //{
            //    Console.WriteLine(family);
            //}
            
            // Now execute the same query via direct SQL
            IQueryable<dynamic> QueryInSql = this.client.CreateDocumentQuery<dynamic>(
                    UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
                    "SELECT top 10 * FROM form4",
                    queryOptions);

            Console.WriteLine("Running direct SQL query...");
            CreateFile(path);
            int count = 0;
            foreach (var doc in QueryInSql)
            {
                var x = JsonConvert.SerializeObject(doc, Formatting.None);
                if (x.Contains("'"))
                {
                    x=x.Replace("'", "''");
                }
                var script="insert into form4 values ('"+x+"');";
                //Console.WriteLine(script);
                GenerateText(script, path);
                count++;
            }
        }
        private static void CreateFile(string path)
        {
            using (StreamWriter sw = File.CreateText(path))
            {

            }
        }
        private static void GenerateText(String data, string path)
        {
            // windows os
            //string path = @"C:/code/cosmosdb_to_postgresql/insert.sh";
            // linux os
            //string path = @"/home/scpex/code/insert.sh";
            // This text is added only once to the file.
            //using (StreamWriter sw = File.CreateText(path))
            //{

            //}

            if (!File.Exists(path))
            {
                // Create a file to write to.
                using (StreamWriter sw = File.CreateText(path))
                {
                    sw.WriteLine(data);
                }
            }
            if (File.Exists(path))
            {
                // Create a file to write to.
                using (StreamWriter sw = File.AppendText(path))
                {
                    sw.WriteLine(data);
                }
            }

            // This text is always added, making the file longer over time
            // if it is not deleted.
            // using (StreamWriter sw = File.AppendText(path))
            // {
            //     sw.WriteLine("This");
            //     sw.WriteLine("is Extra");
            //     sw.WriteLine("Text");
            // }

            // Open the file to read from.
            // using (StreamReader sr = File.OpenText(path))
            // {
            //     string s = "";
            //     while ((s = sr.ReadLine()) != null)
            //     {
            //         Console.WriteLine(s);
            //     }
            // }
        }
    }
}
