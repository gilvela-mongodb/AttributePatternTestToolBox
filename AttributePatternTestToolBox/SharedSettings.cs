using MongoDB.Bson;
using MongoDB.Driver;
using System.Configuration;

namespace MDBW2020AttributeVsWildcard {
  public sealed class SharedSettings {
    //Based on https://csharpindepth.com/articles/singleton

    //Singleton object of the instance
    private static readonly SharedSettings instance = new SharedSettings();

    //Avoids class to be marked as beforefieldinit, makes the singleton as lazy as possible
    static SharedSettings() { }

    //Clients to the MongoDb instances
    private readonly MongoClient classicAttrClient;
    private readonly MongoClient enhancedAttrClient;
    private readonly MongoClient classicSubdocClient;
    private readonly MongoClient wildcardSubdocClient;

    //Databases
    private readonly IMongoDatabase classicAttrDB;
    private readonly IMongoDatabase enhancedAttrDB;
    private readonly IMongoDatabase classicSubdocDB;
    private readonly IMongoDatabase wildcardSubdocDB;

    //Base Collections 
    private readonly IMongoCollection<BsonDocument> classicAttrColl;
    private readonly IMongoCollection<BsonDocument> enhancedAttrColl;
    private readonly IMongoCollection<BsonDocument> classicSubdocColl;
    private readonly IMongoCollection<BsonDocument> wildcardSubdocColl;

    //Results Collections
    private readonly IMongoCollection<BsonDocument> classicAttrResultsColl;
    private readonly IMongoCollection<BsonDocument> enhancedAttrResultsColl;
    private readonly IMongoCollection<BsonDocument> classicSubdocResultsColl;
    private readonly IMongoCollection<BsonDocument> wildcardSubdocResultsColl;

    /// <summary>
    /// Creates a shared settings object from the App.config settings
    /// </summary>
    private SharedSettings() {
      string classicAttrURI = ConfigurationManager.ConnectionStrings["ClassicAttr"].ConnectionString;
      string enhancedAttrURI = ConfigurationManager.ConnectionStrings["EnhancedAttr"].ConnectionString;
      string classicSubdocURI = ConfigurationManager.ConnectionStrings["ClassicSubdoc"].ConnectionString;
      string wildcardSubdocURI = ConfigurationManager.ConnectionStrings["WildcardSubdoc"].ConnectionString;

      string classicAttrDBString = ConfigurationManager.AppSettings["ClassicAttrDB"];
      string enhancedAttrDBString = ConfigurationManager.AppSettings["EnhancedAttrDB"];
      string classicSubdocDBString = ConfigurationManager.AppSettings["ClassicSubdocDB"];
      string wildcardSubdocDBString = ConfigurationManager.AppSettings["WildcardSubdocDB"];

      string classicAttrCollString = ConfigurationManager.AppSettings["ClassicAttrColl"];
      string enhancedAttrCollString = ConfigurationManager.AppSettings["EnhancedAttrColl"];
      string classicSubdocCollString = ConfigurationManager.AppSettings["ClassicSubdocColl"];
      string wildcardSubdocCollString = ConfigurationManager.AppSettings["WildcardSubdocColl"];

      string classicAttrResultsCollString = ConfigurationManager.AppSettings["ClassicAttrResultsColl"];
      string enhancedAttrResultsCollString = ConfigurationManager.AppSettings["EnhancedAttrResultsColl"];
      string classicSubdocResultsCollString = ConfigurationManager.AppSettings["ClassicSubdocResultsColl"];
      string wildcardSubdocResultsCollString = ConfigurationManager.AppSettings["WildcardSubdocResultsColl"];

      //Initializes the connections
      classicAttrClient = new MongoClient(classicAttrURI);
      enhancedAttrClient = new MongoClient(enhancedAttrURI);
      classicSubdocClient = new MongoClient(classicSubdocURI);
      wildcardSubdocClient = new MongoClient(wildcardSubdocURI);

      //gets databases
      classicAttrDB = classicAttrClient.GetDatabase(classicAttrDBString);
      enhancedAttrDB = enhancedAttrClient.GetDatabase(enhancedAttrDBString);
      classicSubdocDB = classicSubdocClient.GetDatabase(classicSubdocDBString);
      wildcardSubdocDB = wildcardSubdocClient.GetDatabase(wildcardSubdocDBString);

      //gets the base collections
      classicAttrColl = classicAttrDB.GetCollection<BsonDocument>(classicAttrCollString);
      enhancedAttrColl = enhancedAttrDB.GetCollection<BsonDocument>(enhancedAttrCollString);
      classicSubdocColl = classicSubdocDB.GetCollection<BsonDocument>(classicSubdocCollString);
      wildcardSubdocColl = wildcardSubdocDB.GetCollection<BsonDocument>(wildcardSubdocCollString);

      //gets the results collections
      classicAttrResultsColl = classicAttrDB.GetCollection<BsonDocument>(classicAttrResultsCollString);
      enhancedAttrResultsColl = enhancedAttrDB.GetCollection<BsonDocument>(enhancedAttrResultsCollString);
      classicSubdocResultsColl = classicSubdocDB.GetCollection<BsonDocument>(classicSubdocResultsCollString);
      wildcardSubdocResultsColl = wildcardSubdocDB.GetCollection<BsonDocument>(wildcardSubdocResultsCollString);
    }

    /// <summary>
    /// Gets the shared settings instance
    /// </summary>
    public static SharedSettings Instance {
      get {
        return instance;
      }
    }

    /// <summary>
    /// Gets the MongoDB client for the Classic Attributes Instance
    /// </summary>
    public MongoClient ClassicAttrClient {
      get {
        return classicAttrClient;
      }
    }

    /// <summary>
    /// Gets the MongoDB client for the Enhanced Attributes Instance
    /// </summary>
    public MongoClient EnhancedAttrClient {
      get {
        return enhancedAttrClient;
      }
    }

    /// <summary>
    /// Gets the MongoDB client for the Classic Subdocument (naive approach) Instance
    /// </summary>
    public MongoClient ClassicSubdocClient {
      get {
        return classicSubdocClient;
      }
    }

    /// <summary>
    /// Gets the MongoDB client for the Wildcard index Instance
    /// </summary>
    public MongoClient WildcardSubdocClient {
      get {
        return wildcardSubdocClient;
      }
    }

    /// <summary>
    /// Gets the Classic Attributes database
    /// </summary>
    public IMongoDatabase ClassicAttrDB {
      get {
        return classicAttrDB;
      }
    }

    /// <summary>
    /// Gets the Enhanced Attributes database
    /// </summary>
    public IMongoDatabase EnhancedAttrDB {
      get {
        return enhancedAttrDB;
      }
    }

    /// <summary>
    /// Gets the Classic Subodcument (naive approach) database
    /// </summary>
    public IMongoDatabase ClassicSubdocDB {
      get {
        return classicSubdocDB;
      }
    }

    /// <summary>
    /// Gets the Wildcard index database
    /// </summary>
    public IMongoDatabase WildcardSubdocDB {
      get {
        return wildcardSubdocDB;
      }
    }

    /// <summary>
    /// Gets the Classic Attributes base collection
    /// </summary>
    public IMongoCollection<BsonDocument> ClassicAttrColl {
      get {
        return classicAttrColl;
      }
    }

    /// <summary>
    /// Gets the Enhanced Attributes base collection
    /// </summary>
    public IMongoCollection<BsonDocument> EnhancedAttrColl {
      get {
        return enhancedAttrColl;
      }
    }

    /// <summary>
    /// Gets the Classic Subdocument (naive approach) base collection
    /// </summary>
    public IMongoCollection<BsonDocument> ClassicSubdocColl {
      get {
        return classicSubdocColl;
      }
    }

    /// <summary>
    /// Gets the Wildcard index base collection
    /// </summary>
    public IMongoCollection<BsonDocument> WildcardSubdocColl {
      get {
        return wildcardSubdocColl;
      }
    }

    /// <summary>
    /// Gets the Classic Attributes results collection
    /// </summary>
    public IMongoCollection<BsonDocument> ClassicAttrResultsColl {
      get {
        return classicAttrResultsColl;
      }
    }

    /// <summary>
    /// Gets the Enhanced Attributes results collection
    /// </summary>
    public IMongoCollection<BsonDocument> EnhancedAttrResultsColl {
      get {
        return enhancedAttrResultsColl;
      }
    }

    /// <summary>
    /// Gets the Classic subdocument (naive approach) results collection
    /// </summary>
    public IMongoCollection<BsonDocument> ClassicSubdocResultsColl {
      get {
        return classicSubdocResultsColl;
      }
    }

    /// <summary>
    /// Gets the Wildcard index results collection
    /// </summary>
    public IMongoCollection<BsonDocument> WildcardSubdocResultsColl {
      get {
        return wildcardSubdocResultsColl;
      }
    }
  }
}
