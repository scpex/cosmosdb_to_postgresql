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
        static int page = 1;
        static int count = 1;
        static string pagestr = Convert.ToString(page);
        // windows os
        string path = @"C:\code\cosmosdb_to_postgresql\doc{0}.csv";
        // linux os
        //string path = @"/home/scpex/code/insert{0}.sh";
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

            //int pageSize = 100;
            //for (int i = 0; i <= 40; i++)
            //{
                
            //    IQueryable<dynamic> QueryInSql = this.client.CreateDocumentQuery<dynamic>(
            //    UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
            //    queryOptions).Take(pageSize).Skip(5);

            //    Console.WriteLine("Running direct SQL query...");
            //    string newFilepath = string.Format(path, i);
            //    Console.WriteLine("Createfile .." + newFilepath);
            //    CreateFile(newFilepath);
            //    foreach (var doc in QueryInSql)
            //    {
            //        var x = JsonConvert.SerializeObject(doc, Formatting.None);
            //        if (x.Contains("'"))
            //        {
            //            x = x.Replace("'", "''");
            //        }
            //        var script = "insert into form4 values ('" + x + "');";
            //        //Console.WriteLine(script);
            //        GenerateText(script, newFilepath);
            //    }
            //}

            // Now execute the same query via direct SQL
            IQueryable<dynamic> QueryInSql = this.client.CreateDocumentQuery<dynamic>(
                    UriFactory.CreateDocumentCollectionUri(databaseName, collectionName),
                    "SELECT * FROM form4",
                    queryOptions);

            Console.WriteLine("Running direct SQL query...");
            string json_result = "";
            foreach (var doc in QueryInSql)
            {
                var json_record = JsonConvert.SerializeObject(doc, Formatting.None);
                if (json_record.Contains("'"))
                {
                    json_record = json_record.Replace("'", "''");
                }
                if (json_record.Contains("\\\""))
                {
                    json_record = json_record.Replace("\\\"", "\"");
                }
                if (json_record.Contains("etag\":\"\""))
                {
                    json_record = json_record.Replace("\"\"", "\"");
                    if (json_record.Contains("\":\","))
                    {
                        json_record = json_record.Replace("\":\",", "\":\"\",");
                        if (json_record.Contains(":\"}"))
                        {
                            json_record = json_record.Replace(":\"}", ":\"\"}");
                            if (json_record.Contains("\"group\""))
                            {
                                json_record = json_record.Replace("\"group\"", "group");
                            }
                        }
                    }
                }
                json_result += json_record+'\n';
                //Console.WriteLine(json_record);
                count++;
                if (count%100 == 0)
                {
                    string newFilepath = string.Format(path, page);
                    GenerateText(json_result, newFilepath);
                    page++;
                }
            }
        }
        private static void CreateFile(string path)
        {
            using (StreamWriter sw = File.CreateText(path))
            {
                string newFilepath = string.Format(path, page);
                Console.WriteLine("Createfile .." + newFilepath);
            }
        }
        private static void GenerateText(string data, string path)
        {
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
