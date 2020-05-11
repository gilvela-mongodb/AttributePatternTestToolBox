using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;

namespace MDBW2020AttributeVsWildcard {
  public class DataLoader {

    //Parameters for the job
    private readonly BsonDocument documentTemplate;
    private readonly int batchSize;
    private readonly int batchCount;

    //Index definitions
    private readonly BsonArray classicAttrIdx;
    private readonly BsonArray enhancedAttrIdx;
    private readonly BsonArray classicSubdocIdx;
    private readonly BsonArray wildcardSubdocIdx;

    //Document generator
    private readonly DocumentGenerator documentGenerator;

    //Shared Settings
    private readonly SharedSettings sharedSettings;

    /// <summary>
    /// Initializes the loader, loads the settings from App.config, creates the connections to the database and 
    /// initializes the document generator
    /// </summary>
    public DataLoader() {
      //Gets the shared settings
      sharedSettings = SharedSettings.Instance;

      //Remaining settings:
      //index definitions
      //template
      //batch size
      //number of batches
      string documentTemplateJSON = ConfigurationManager.AppSettings["DocumentTemplate"];
      string batchSizeString = ConfigurationManager.AppSettings["InsertBatchSize"];
      string batchCountString = ConfigurationManager.AppSettings["InsertBatchCount"];

      string classicAttrIdxJSON = ConfigurationManager.AppSettings["ClassicAttrIdx"];
      string enhancedAttrIdxJSON = ConfigurationManager.AppSettings["EnhancedAttrIdx"];
      string classicSubdocIdxJSON = ConfigurationManager.AppSettings["ClassicSubdocIdx"];
      string wildcardSubdocIdxJSON = ConfigurationManager.AppSettings["WildcardSubdocIdx"];

      //gets the template as Bson and initializes the counters
      documentTemplate = BsonDocument.Parse(documentTemplateJSON);
      batchSize = int.Parse(batchSizeString);
      batchCount = int.Parse(batchCountString);

      //parses the indexes
      classicAttrIdx = BsonSerializer.Deserialize<BsonArray>(classicAttrIdxJSON);
      enhancedAttrIdx = BsonSerializer.Deserialize<BsonArray>(enhancedAttrIdxJSON);
      classicSubdocIdx = BsonSerializer.Deserialize<BsonArray>(classicSubdocIdxJSON);
      wildcardSubdocIdx = BsonSerializer.Deserialize<BsonArray>(wildcardSubdocIdxJSON);

      documentGenerator = new DocumentGenerator();
    }

    /// <summary>
    /// Loads a batch of documents into MongoDB
    /// </summary>
    /// <typeparam name="T">Type of the document</typeparam>
    /// <param name="collection">Collection where to load the documents</param>
    /// <param name="batch">List of documents to load as a batch</param>
    /// <param name="id">Identifier of the batch</param>
    private static void LoadBatch<T>(IMongoCollection<T> collection, List<T> batch, string id) {
      Console.Out.WriteLine(string.Format("Starting batch {0} for {1} documents", id, batch.Count));
      try {
        collection.InsertMany(batch);
      } catch (Exception ex) {
        Console.Error.WriteLine(string.Format("Error at batch {0}.", id));
        Console.Error.WriteLine(ex.StackTrace);
        throw ex;
      }
      Console.Out.WriteLine(string.Format("Batch {0} done.", id));
    }

    /// <summary>
    /// Creates batches of documents and loads it to the different databases
    /// </summary>
    /// <param name="template">Document template in json format</param>
    /// <param name="size">Number of documents to load</param>
    /// <param name="id">Identifier of the batch</param>
    private void LoadBatch(BsonDocument template, int size, string id) {
      List<BsonDocument> arrayDocs = new List<BsonDocument>();
      List<BsonDocument> subdocDocs = new List<BsonDocument>();

      //Gets the batch from the generator
      documentGenerator.getBatchOfDocuments(template, size, subdocDocs, arrayDocs);

      //Runs the queries in parallel :D
      Parallel.Invoke(new Action[]{
        () => {
          LoadBatch<BsonDocument>(sharedSettings.ClassicAttrColl, arrayDocs, id + "_ClassicAttr");
        },
        () => {
          LoadBatch<BsonDocument>(sharedSettings.EnhancedAttrColl, arrayDocs, id + "_EnhancedAttr");
        },
        () => {
          LoadBatch<BsonDocument>(sharedSettings.ClassicSubdocColl, subdocDocs, id + "_ClassicSubdoc");
        },
        () => {
          LoadBatch<BsonDocument>(sharedSettings.WildcardSubdocColl, subdocDocs, id + "_WildcardSubdoc");
        }
      });

    }

    /// <summary>
    /// Creates a collection (drops first if it exists), and creates its indexes
    /// </summary>
    /// <param name="coll">Collection object</param>
    /// <param name="indexes"></param>
    private static void setupCollection<T>(IMongoCollection<T> coll, BsonArray indexes) {   
      //drops the collection
      coll.Database.DropCollection(coll.CollectionNamespace.CollectionName);

      //creates each index
      foreach (BsonValue idx in indexes) {
        #pragma warning disable 0618
        coll.Indexes.CreateOne(idx.AsBsonDocument);
      }
    }

    public void Main() {
      //Initializes the collections
      setupCollection<BsonDocument>(sharedSettings.ClassicAttrColl, classicAttrIdx);
      setupCollection<BsonDocument>(sharedSettings.EnhancedAttrColl, enhancedAttrIdx);
      setupCollection<BsonDocument>(sharedSettings.ClassicSubdocColl, classicSubdocIdx);
      setupCollection<BsonDocument>(sharedSettings.WildcardSubdocColl, wildcardSubdocIdx);

      Console.Out.WriteLine("Starting load.");

      //Does the magic xD
      //Tries to run in parallel as many batches as possible up to the number specified in configuration 
      Parallel.For(0, batchCount, i => {
        LoadBatch(documentTemplate, batchSize, i.ToString());
      });

      Console.Out.WriteLine("Load done.");
    }
  }
}
