using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;

namespace MDBW2020AttributeVsWildcard {
  public class EqualityBenchmark {

    private readonly SharedSettings sharedSettings;

    //Query template, will be used to get the proper query shapes
    private readonly BsonDocument queryTemplate;
    //Number of queries to execute
    private readonly int testCount;
    //Maximum number of parallel tasks
    private readonly int parallelCount;
    //Number of attributes in a single query
    private readonly int attributesToQuery;

    //Query shape generator
    private readonly DocumentGenerator documentGenerator;

    //Sets of query documents based on shape
    private readonly List<BsonDocument> classicAttributeQueries;
    private readonly List<BsonDocument> enhancedAttributeQueries;
    private readonly List<BsonDocument> dotNotationQueries;

    //name of the attrbiute field
    private const string ATTRIBUTES = "attributes";

    //Randomness generator
    private readonly Random rnd;

    /// <summary>
    /// Creates the EqualityBenchmark object
    /// </summary>
    public EqualityBenchmark() {
      //Gets the shared settings
      sharedSettings = SharedSettings.Instance;

      //Reads settings for the test
      string queryTemplateJSON = ConfigurationManager.AppSettings["EqualityQueryTemplate"];
      string testCountString = ConfigurationManager.AppSettings["EqualityTestCount"];
      string parallelCountString = ConfigurationManager.AppSettings["EqualityParallelCount"];
      string attributesToQueryString = ConfigurationManager.AppSettings["AttributesToQuery"];

      //parses the settings
      queryTemplate = BsonDocument.Parse(queryTemplateJSON);
      testCount = int.Parse(testCountString);
      parallelCount = int.Parse(parallelCountString);
      attributesToQuery = int.Parse(attributesToQueryString);

      //gets the generator of query documents
      documentGenerator = new DocumentGenerator();

      //initalizes the query lists
      classicAttributeQueries = new List<BsonDocument>(testCount);
      enhancedAttributeQueries = new List<BsonDocument>(testCount);
      dotNotationQueries = new List<BsonDocument>(testCount);

      rnd = new Random();
    }

    /// <summary>
    /// Executes a query on a specific collection. It gets the executionStats, then it gets the results
    /// and then saves the results on a second collection
    /// </summary>
    /// <param name="baseColl">Collection on which the query run</param>
    /// <param name="resultsColl">Collection where the result will be saved</param>
    /// <param name="query">Query/Filter document</param>
    /// <param name="id">String specifying this specific run</param>
    private void ExecuteQuery(
      IMongoCollection<BsonDocument> baseColl,
      IMongoCollection<BsonDocument> resultsColl,
      BsonDocument query,
      String id,
      String type) {

      Console.Out.WriteLine(String.Format("Starting run {0}, type: {1}", id, type));

      //First run the explain
      BsonDocument explainResults = GetExecutionStats(baseColl, query);

      BsonArray _ids = new BsonArray();

      //Executes the query and saves the _ids of each matched document
      foreach (BsonDocument x in baseColl.Find<BsonDocument>(query).ToEnumerable()) {
        _ids.Add(x["_id"]);
      }

      //Creates the ouput
      BsonDocument output = new BsonDocument("_id", id);

      //adds the query document
      output["query"] = DataTransform.FixFieldNames(query.DeepClone().AsBsonDocument);

      //adds the number of matched documents
      output["nMatched"] = _ids.Count;

      //adds the matched ids
      output["matched_ids"] = _ids;

      //adds the execution stats
      output["executionStats"] = DataTransform.FixFieldNames(explainResults);

      //Writes the result :)
      resultsColl.InsertOne(output);

      Console.Out.WriteLine(String.Format("Finished run {0}, type: {1}", id, type));
    }

    /// <summary>
    /// Gets the execution stats for a specific query shape
    /// </summary>
    /// <param name="coll">Collection on which the query runs</param>
    /// <param name="query">Query/filter document</param>
    /// <returns>
    /// Document with the output of the explain plan with berbosity of execution stats for the specified query
    /// </returns>
    private BsonDocument GetExecutionStats(IMongoCollection<BsonDocument> coll, BsonDocument query) {
      //Creates the find command document as it is a requirement for the explain command document
      BsonDocument findCommand = new BsonDocument("find", coll.CollectionNamespace.CollectionName);
      findCommand["filter"] = query;

      //Creates the explain command
      BsonDocument explainCommand = new BsonDocument("explain", findCommand);
      explainCommand["verbosity"] = "executionStats";

      //Runs the explain and returns the result
      return coll.Database.RunCommand<BsonDocument>(explainCommand);
    }

    /// <summary>
    /// Holds a batch of query documents for the 3 query shapes 
    /// </summary>
    private class QueryDocumentBatch {
      public List<BsonDocument> classicAttributeQueries;
      public List<BsonDocument> enhancedAttributeQueries;
      public List<BsonDocument> dotNotationQueries;

      /// <summary>
      /// Initializes the lists to hold the query documents
      /// </summary>
      /// <param name="size">size of the list</param>
      public QueryDocumentBatch(int size) {
        classicAttributeQueries = new List<BsonDocument>(size);
        enhancedAttributeQueries = new List<BsonDocument>(size);
        dotNotationQueries = new List<BsonDocument>(size);
      }
    }

    /// <summary>
    /// Changes a query template so it only has the number of attributes specified in the configuration
    /// </summary>
    /// <param name="input">A BsonDocument with only the number of attributes specified</param>
    /// <returns>
    /// The same document as the input
    /// </returns>
    /// <remarks>
    /// If the current template has less elements than the ones specified in the configuration, the template is left
    /// untouched.
    /// </remarks>
    private BsonDocument setQueryAttributes(BsonDocument input) {
      //checks if the input has a subdocument, if not just get out of here
      BsonValue attributes = input[ATTRIBUTES];
      if (attributes == null || !attributes.IsBsonDocument) {
        return input;
      }
      BsonDocument doc = attributes.AsBsonDocument;

      //Determines how many elements to remove from the query, by getting the current count minus
      //the elements specified to be queried
      int elementsToRemove = doc.ElementCount - attributesToQuery;

      //removes the elements
      for (int i = 0; i < elementsToRemove; i++) {
        doc.RemoveAt(rnd.Next(doc.ElementCount));
      }

      //Just in case sets the attributes field to the array, this should be NO OP
      input[ATTRIBUTES] = doc;

      //returns the input
      return input;
    }

    /// <summary>
    /// Gets a batch of query documents for the 3  query shapes: dot notation, classic attribute, and enhanced 
    /// attribute
    /// </summary>
    /// <param name="size">
    /// Size of the batch
    /// </param>
    /// <returns></returns>
    private QueryDocumentBatch GetQueryDocumentBatch(int size) {
      QueryDocumentBatch output = new QueryDocumentBatch(size);

      for (int i = 0; i < size; i++) {
        //gets a random query template
        BsonDocument baseDocument = documentGenerator.GetBaseDocument(queryTemplate, false);

        //Processes the template so it only has the number of attributes specified in the config
        setQueryAttributes(baseDocument);

        //Converts into a valid query and saves in the proper 
        output.classicAttributeQueries.Add(DataTransform.ConvertSubDocumentToAttributeQuery(baseDocument));
        output.enhancedAttributeQueries.Add(DataTransform.ConvertSubDocumentToEnhancedAttributeQuery(baseDocument));
        output.dotNotationQueries.Add(DataTransform.ConvertSubDocumentToDotNotationQuery(baseDocument));
      }
      return output;
    }

    /// <summary>
    /// Executes the entire query set for a determined query shape.
    /// </summary>
    /// <param name="baseColl">Collection on where the queries will run</param>
    /// <param name="resultsColl">Collection to save the results of the execution</param>
    /// <param name="queries">Set of queries</param>
    /// <param name="idPrefix">String specifying this specific run</param>
    /// <param name="type">Identifier of the type of query shape</param>
    /// <returns></returns>
    private void ExecuteQueries(
      IMongoCollection<BsonDocument> baseColl,
      IMongoCollection<BsonDocument> resultsColl,
      List<BsonDocument> queries,
      string idPrefix,
      string type) {

      // Determine the possible number of parallelism for this pattern by dividing the number of maximum possible 
      //threads by four
      ParallelOptions opt = new ParallelOptions();
      opt.MaxDegreeOfParallelism = parallelCount / 4;

      //Runs all posible queries in parallel 
      Parallel.For(0, queries.Count, opt, (counter) => {
        string id = string.Format("{0}_{1}", idPrefix, counter);
        ExecuteQuery(baseColl, resultsColl, queries[counter], id, type);
      });
    }

    /// <summary>
    /// Equality Benchmark main logic
    /// </summary>
    public void Main() {

      //Gets the Unix timestamp so we can use it as id
      string unixTimestamp = Math.Floor((DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds).ToString();

      Console.Out.WriteLine("Starting Benchmark.");
      Console.Out.WriteLine(String.Format("Timestamp of the benchmark: {0}", unixTimestamp));

      //Gets the query shapes
      QueryDocumentBatch batch = GetQueryDocumentBatch(testCount);
      classicAttributeQueries.AddRange(batch.classicAttributeQueries);
      enhancedAttributeQueries.AddRange(batch.enhancedAttributeQueries);
      dotNotationQueries.AddRange(batch.dotNotationQueries);

      //Runs the for types of queries in parallel
      Parallel.Invoke(new Action[] {
        () => {
          ExecuteQueries(sharedSettings.ClassicAttrColl, sharedSettings.ClassicAttrResultsColl, 
            classicAttributeQueries, unixTimestamp, "Classic Attribute");
        },
        () => {
          ExecuteQueries(sharedSettings.EnhancedAttrColl, sharedSettings.EnhancedAttrResultsColl, 
            enhancedAttributeQueries, unixTimestamp, "Enhanced Attribute");
        },
        () => {
          ExecuteQueries(sharedSettings.ClassicSubdocColl, sharedSettings.ClassicSubdocResultsColl,
            dotNotationQueries, unixTimestamp, "Classic Subdocument");
        },
        () => {
          ExecuteQueries(sharedSettings.WildcardSubdocColl, sharedSettings.WildcardSubdocResultsColl,
            dotNotationQueries, unixTimestamp, "Wildcard Index");
        }
      });

      Console.Out.WriteLine("Benchmark finished.");
    }

  }
}
