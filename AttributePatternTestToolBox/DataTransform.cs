using MongoDB.Bson;
using System;
using System.Linq;

namespace MDBW2020AttributeVsWildcard {
  class DataTransform {

    //name of key and value fields
    private const string K = "k";
    private const string V = "v";

    //name of the attrbiute field
    private const string ATTRIBUTES = "attributes";

    /// <summary>
    /// Converts a document that has a subdocument with variable attributes to a document with an array of attributes
    /// </summary>
    /// <param name="input">Document with a field named attribute that is a subdocument</param>
    /// <returns>
    /// Document that is a copy of the passed one, except the attributes field is converted from a subdocument
    /// to an array of attributes
    /// </returns>
    public static BsonDocument ConvertSubDocumentToAttributeArray(BsonDocument input) {
      BsonDocument output = new BsonDocument();
      //iterates thru each field
      foreach (BsonElement field in input) {
        if (field.Name != ATTRIBUTES) {
          // If the field is not attribute copies its value as is
          output[field.Name] = field.Value;
        } else {
          //If this is the attrbibute field it needs to be a subdocument
          if (!field.Value.IsBsonDocument) {
            throw new Exception("The attributes field is not a subdocument.");
          }
          BsonArray attributes = new BsonArray();
          //Gets the subfields and convert them to an attribute array
          foreach (BsonElement attrs in field.Value.AsBsonDocument) {
            attributes.Add(new BsonDocument().Add(K, attrs.Name).Add(V, attrs.Value));
          }
          output[field.Name] = attributes;
        }
      }
      return output;
    }

    /// <summary>
    /// Converts a query document that has a subdocument with attributes to a query document with the proper dot 
    /// notation for the attributes in the subdocument
    /// </summary>
    /// <param name="input">Document with a field named attribute that is a subdocument</param>
    /// <returns>
    /// Documents that is a copy of the passed one, except the attributes field is replaced with multiple fields with
    /// dot notation for proper querying
    /// </returns>
    public static BsonDocument ConvertSubDocumentToDotNotationQuery(BsonDocument input) {
      BsonDocument output = new BsonDocument();
      //iterates thru each field
      foreach (BsonElement field in input) {
        if (field.Name != ATTRIBUTES) {
          // If the field is not attribute copies its value as is
          output[field.Name] = field.Value;
        } else {
          //If this is the attrbibute field it needs to be a subdocument
          if (!field.Value.IsBsonDocument) {
            throw new Exception("The attributes field is not a subdocument.");
          }
          BsonArray attributes = new BsonArray();
          //Gets the subfields and add each one of them as dot notation
          foreach (BsonElement attrs in field.Value.AsBsonDocument) {
            output.Add(ATTRIBUTES + "." + attrs.Name, attrs.Value);
          }
        }
      }
      return output;
    }

    /// <summary>
    /// Converts a document that has a subdocument with variable attributes to a document with an $and operator
    /// and one entry per attribute in the $and array. Each element in the array will be using $elemMatch to ensure
    /// matching the right attribute.
    /// </summary>
    /// <param name="input">Document with a field named attribute that is a subdocument</param>
    /// <returns>
    /// Document that is a copy of the passed one, except the attributes field is converted to an $and operator, with
    /// each element an element of the $and operator
    /// </returns>
    public static BsonDocument ConvertSubDocumentToAttributeQuery(BsonDocument input) {
      BsonDocument output = new BsonDocument();
      //iterates thru each field
      foreach (BsonElement field in input) {
        if (field.Name != ATTRIBUTES) {
          // If the field is not attribute copies its value as is
          output[field.Name] = field.Value;
        } else {
          //If this is the attrbibute field it needs to be a subdocument
          if (!field.Value.IsBsonDocument) {
            throw new Exception("The attributes field is not a subdocument.");
          }
          BsonArray elements = new BsonArray();
          //Gets the subfields and add each one of them as dot notation
          foreach (BsonElement attrs in field.Value.AsBsonDocument) {
            elements.Add(new BsonDocument(
              ATTRIBUTES, new BsonDocument("$elemMatch", new BsonDocument(K, attrs.Name).Add(V, attrs.Value))));
          }
          output["$and"] = elements;
        }
      }
      return output;
    }


    /// <summary>
    /// Converts a document that has a subdocument with variable attributes to a document with an $and operator
    /// and one entry per attribute in the $and array. Each element in the array will be using full document match.
    /// </summary>
    /// <param name="input">Document with a field named attribute that is a subdocument</param>
    /// <returns>
    /// Document that is a copy of the passed one, except the attributes field is converted to an $and operator, with
    /// each element an element of the $and operator
    /// </returns>
    public static BsonDocument ConvertSubDocumentToEnhancedAttributeQuery(BsonDocument input) {
      BsonDocument output = new BsonDocument();
      //iterates thru each field
      foreach (BsonElement field in input) {
        if (field.Name != ATTRIBUTES) {
          // If the field is not attribute copies its value as is
          output[field.Name] = field.Value;
        } else {
          //If this is the attrbibute field it needs to be a subdocument
          if (!field.Value.IsBsonDocument) {
            throw new Exception("The attributes field is not a subdocument.");
          }
          BsonArray elements = new BsonArray();
          //Gets the subfields and add each one of them as dot notation
          foreach (BsonElement attrs in field.Value.AsBsonDocument) {
            elements.Add(new BsonDocument(ATTRIBUTES, new BsonDocument(K, attrs.Name).Add(V, attrs.Value)));
          }
          output["$and"] = elements;
        }
      }
      return output;
    }

    /// <summary>
    /// Removes dollar signs and dot from fieldnames so a document can be saved to the database
    /// </summary>
    /// <param name="input">Any BsonDocument</param>
    /// <returns>The same document with its field names fixed</returns>
    public static BsonDocument FixFieldNames(BsonDocument input) {
      for (int i = 0; i < input.ElementCount; i++) {
        BsonElement x = input.Elements.ElementAt(i);
        bool hasChanged = false;
        BsonValue value;
        string name;

        //If the value is a subdocument we parse it and we assume there was a change
        if (x.Value.IsBsonDocument) {
          value = FixFieldNames(x.Value.AsBsonDocument);
          hasChanged = true;
        } else {
          value = x.Value;
        }

        //if the value is an array, we check if any of the elements is a subdoc and we fix it
        if (x.Value.IsBsonArray) {
          BsonArray array = x.Value.AsBsonArray;
          for (int j = 0; j < array.Count; j++) {
            if (array[j].IsBsonDocument) {
              array[j] = FixFieldNames(array[j].AsBsonDocument);
              hasChanged = true;
            }
          }
          value = array;

        }

        name = x.Name;
        //checks if the field name has a dollar sign, if so, replaces with underscore
        if (name.Contains("$")) {
          name = name.Replace('$', '_');
          hasChanged = true;
        }

        //checks if the field name has a dor, if so replaces with a comma
        if (name.Contains(".")) {
          name = name.Replace('.', ',');
          hasChanged = true;
        }

        //if there were changes we change the key value pair, otherwise let it be
        if (hasChanged) {
          input.SetElement(i, new BsonElement(name, value));
        }
      }
      return input;
    }

  }
}
