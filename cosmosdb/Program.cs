﻿using System;

// ADD THIS PART TO YOUR CODE
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace cosmosdb
{
    class Program
    {
        // ADD THIS PART TO YOUR CODE
        private const string EndpointUri = "https://equityinsights.documents.azure.com:443/";
        private const string PrimaryKey = "txClUBsd1i2tQU5PXZXT6i642pJCVCyxDd32LOREJmZcHVUyT4DJFfBDLdnE2xcuzWeU6UozBNAAVce9F20jkw==";
        private DocumentClient client;

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
                    "SELECT count c.id FROM form4 c",
                    queryOptions);

            Console.WriteLine("Running direct SQL query...");
            foreach (dynamic family in QueryInSql)
            {
                Console.WriteLine("\tRead {0}", family);
            }

            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
        }
    }
}