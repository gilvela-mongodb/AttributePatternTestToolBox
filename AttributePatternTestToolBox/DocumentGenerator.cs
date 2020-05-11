using System;
using System.Collections.Generic;
using MongoDB.Bson;
using System.Configuration;

namespace MDBW2020AttributeVsWildcard {
  public class DocumentGenerator {

    //String Catalog
    private readonly string[] stringCatalog;

    //Maximum value for the integer fields, minimum is 0
    private readonly int maxInt;

    //Minimum and maximum values for the DateTime fields
    private readonly DateTime baseDate;
    private readonly long maximumSeconds;

    //Randomness generator
    private Random rnd;

    //name of the attrbiute field
    private const string ATTRIBUTES = "attributes";

    /// <summary>
    /// Creates a new DocumentGenerator
    /// </summary>
    public DocumentGenerator() {
      //initializes the document generator
      string[] stringCatalog = ConfigurationManager.AppSettings["StringCatalog"].Split(new char[] { ',' });
      for (int i = 0; i < stringCatalog.Length; i++) {
        stringCatalog[i] = stringCatalog[i].Trim();
      }
      this.stringCatalog = stringCatalog;

      maxInt = int.Parse(ConfigurationManager.AppSettings["MaxInt"]);
      baseDate = DateTime.Parse(
        ConfigurationManager.AppSettings["MinimumDate"], null, System.Globalization.DateTimeStyles.RoundtripKind);

      DateTime maximumDate = DateTime.Parse(
        ConfigurationManager.AppSettings["MaximumDate"], null, System.Globalization.DateTimeStyles.RoundtripKind);

      maximumSeconds = (maximumDate - baseDate).Seconds;

      rnd = new Random();
    }

    /// <summary>
    /// Creates abatches of documents in the attribute subdocument and array shape for a given template
    /// </summary>
    /// <param name="template">
    /// The document must have a list of scalar fields, the name and data types of these fields will be the same in the
    /// output documents with random values. The document must have a field called attributes that should be a 
    /// subdocument and all its fields should be scalar (no arrays, no subdocuments).
    /// </param>
    /// <param name="size">Size of the batch</param>
    /// <param name="subdocDocs">Non null List where the subdocument documents will be stored</param>
    /// <param name="arrayDocs">Non null List where the array documents will be stored</param>
    public void getBatchOfDocuments(
     BsonDocument template, int size, List<BsonDocument> subdocDocs, List<BsonDocument> arrayDocs) {
      for (int i = 0; i < size; i++) {
        BsonDocument subdocDoc = GetBaseDocument(template, true);
        subdocDocs.Add(subdocDoc);
        BsonDocument arrDoc = DataTransform.ConvertSubDocumentToAttributeArray(subdocDoc);
        arrayDocs.Add(arrDoc);
      }
    }

    /// <summary>
    /// Generates a document based on a template. It will take a template document, it will take field names
    /// and data types.
    /// </summary>
    /// <param name="template">
    /// The document must have a list of scalar fields, the name and data types of these fields will be the same in the
    /// output document with random values. The document must have a field called attribute that should be a 
    /// subdocument and all its fields should be scalar (no arrays, no subdocuments).
    /// </param>
    /// <param name="add_id">
    /// If true adds an _id
    /// </param>
    /// <returns>
    /// A document with random values based on the template.
    /// </returns>
    /// <exception cref="Exception">
    /// An exception will be throw if the attribute field is not present or if any field (except for the attribute 
    /// field) is not a: null, bool, int32, int64, double, decimal, datetime, string
    /// </exception>
    public BsonDocument GetBaseDocument(BsonDocument template, Boolean add_id) {
      BsonDocument output = new BsonDocument();
      //iterates thru each field
      foreach (BsonElement field in template) {
        if (field.Name != ATTRIBUTES) {
          // If the field is not attribute get a random value based on the current type
          output[field.Name] = GetRandomValue(field.Value.BsonType);
        } else {
          //If this is the attrbibute field it needs to be a subdocument
          if (!field.Value.IsBsonDocument) {
            throw new Exception("The attributes field is not a subdocument.");
          }
          BsonDocument attributes = new BsonDocument();
          //Gets the subfields and obtains random values for the fields
          foreach (BsonElement attrs in field.Value.AsBsonDocument) {
            attributes[attrs.Name] = GetRandomValue(attrs.Value.BsonType);
          }
          output[field.Name] = attributes;
        }
        //overrides _id
        if (add_id) {
          output["_id"] = new BsonObjectId(ObjectId.GenerateNewId());
        }
      }
      return output;
    }

    /// <summary>
    /// Generates a random value for the specified BsonType.
    /// </summary>
    /// <param name="type">
    /// The BsonType for which a random value will be generated.
    /// </param>
    /// <returns>
    /// A BSON value of the type specified.
    /// </returns>
    /// <exception cref="Exception">
    /// Only null, bool, int, long int, double, decimal, date and string are defined, all other data types will throw an
    /// exception.
    /// </exception>
    private BsonValue GetRandomValue(BsonType type) {
      switch (type) {
        case BsonType.Null:
          return BsonNull.Value;

        case BsonType.Binary:
          return new BsonBoolean(rnd.Next() % 1 == 0);

        case BsonType.Int32:
          return new BsonInt32(rnd.Next(maxInt));

        case BsonType.Int64:
          return new BsonInt64((long)rnd.Next());

        case BsonType.Double:
          return new BsonDouble(rnd.NextDouble());

        case BsonType.Decimal128:
          return BsonDecimal128.Create(rnd.NextDouble());

        case BsonType.DateTime:
          return new BsonDateTime(baseDate.AddSeconds(rnd.NextDouble() * maximumSeconds));

        case BsonType.String:
          return new BsonString(stringCatalog[rnd.Next() % stringCatalog.Length]);

        default:
          throw new Exception("Unsupported BSON data type.");
      }
    }

  }
}
