using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace ContractNotifyCollector.helper
{
    /// <summary>
    /// mongdb 操作帮助类
    /// 
    /// </summary>
    class MongoDBHelper
    {
        public long GetDataCount(string mongodbConnStr, string mongodbDatabase, string coll, string findson = "{}")
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(coll);

            var txCount = collection.Find(BsonDocument.Parse(findson)).CountDocuments();

            client = null;

            return txCount;
        }
        public JArray GetData(string mongodbConnStr, string mongodbDatabase, string coll, string findFliter = "{}", string sortFliter = "{}", int skip = 0, int limit = 0)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(coll);

            List<BsonDocument> query = null;
            if (limit == 0)
            {
                query = collection.Find(BsonDocument.Parse(findFliter)).Sort(sortFliter).Skip(skip).ToList();
            }
            else
            {
                query = collection.Find(BsonDocument.Parse(findFliter)).Sort(sortFliter).Skip(skip).Limit(limit).ToList();
            }
            client = null;

            if (query != null && query.Count > 0)
            {
                var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Strict };
                JArray JA = JArray.Parse(query.ToJson(jsonWriterSettings));
                foreach (JObject j in JA)
                {
                    j.Remove("_id");
                }
                return JA;
            }
            else { return new JArray(); }
        }

        public List<T> GetData<T>(string mongodbConnStr, string mongodbDatabase, string coll, string findFliter = "{}", string sortFliter = "{}", int skip = 0, int limit = 0)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<T>(coll);

            List<T> query = null;
            if (limit == 0)
            {
                query = collection.Find(BsonDocument.Parse(findFliter)).ToList();
            }
            else
            {
                query = collection.Find(BsonDocument.Parse(findFliter)).Sort(sortFliter).Skip(skip).Limit(limit).ToList();
            }
            client = null;

            return query;
        }


        public void PutData(string mongodbConnStr, string mongodbDatabase, string coll, string data, bool isAync = false)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(coll);
            if (isAync)
            {
                collection.InsertOneAsync(BsonDocument.Parse(data));
            }
            else
            {
                collection.InsertOne(BsonDocument.Parse(data));
            }

            collection = null;
        }
        
        public void PutData<T>(string mongodbConnStr, string mongodbDatabase, string coll, T data, bool isAync = false)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<T>(coll);
            if (isAync)
            {
                collection.InsertOneAsync(data);
            }
            else
            {
                collection.InsertOne(data);
            }

            collection = null;
        }
        public void PutData(string mongodbConnStr, string mongodbDatabase, string coll, JArray Jdata)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(coll);

            List<BsonDocument> bsons = new List<BsonDocument>();
            foreach (JObject J in Jdata)
            {
                string strData = Newtonsoft.Json.JsonConvert.SerializeObject(J);
                BsonDocument bson = BsonDocument.Parse(strData);
                bsons.Add(bson);
            }
            collection.InsertMany(bsons.ToArray());

            client = null;
        }

        public void UpdateData(string mongodbConnStr, string mongodbDatabase, string coll, string Jdata, string Jcondition)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(coll);
            collection.UpdateOne(BsonDocument.Parse(Jcondition), BsonDocument.Parse(Jdata));

            client = null;
        }

        public void ReplaceData<T>(string mongodbConnStr, string mongodbDatabase, string coll, T Jdata, string Jcondition)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<T>(coll);

            collection.ReplaceOne(BsonDocument.Parse(Jcondition), Jdata);

            client = null;
        }
        public void ReplaceData(string mongodbConnStr, string mongodbDatabase, string coll, string Jdata, string Jcondition)
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(coll);
            
            collection.ReplaceOne(BsonDocument.Parse(Jcondition), BsonDocument.Parse(Jdata));

            client = null;
        }
        public void ReplaceData(string mongodbConnStr, string mongodbDatabase, string coll, JObject Jdata, JObject Jcondition)
        {
            ReplaceData(mongodbConnStr, mongodbDatabase, coll, Jdata.ToString(), Jcondition.ToString());
        }

        public JArray GetDataWithField(string mongodbConnStr, string mongodbDatabase, string coll, string fieldBson, string findBson = "{}")
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(coll);

            List<BsonDocument> query = collection.Find(BsonDocument.Parse(findBson)).Project(BsonDocument.Parse(fieldBson)).ToList();
            client = null;

            if (query.Count > 0)
            {
                var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Strict };
                JArray JA = JArray.Parse(query.ToJson(jsonWriterSettings));
                /*
                foreach (JObject j in JA)
                {
                    j.Remove("_id");
                }*/
                return JA;
            }
            else { return new JArray(); }
        }
        public JArray GetDataPagesWithField(string mongodbConnStr, string mongodbDatabase, string coll, string fieldBson, int pageCount, int pageNum, string sortBson, string findBson = "{}")
        {
            var client = new MongoClient(mongodbConnStr);
            var database = client.GetDatabase(mongodbDatabase);
            var collection = database.GetCollection<BsonDocument>(coll);

            List<BsonDocument> query = null;
            if (pageCount == 0 || pageNum == 0) {
                query = collection.Find(BsonDocument.Parse(findBson)).Project(BsonDocument.Parse(fieldBson)).Sort(BsonDocument.Parse(sortBson)).ToList();
            }
            else
            {
                query = collection.Find(BsonDocument.Parse(findBson)).Project(BsonDocument.Parse(fieldBson)).Sort(BsonDocument.Parse(sortBson)).Skip(pageCount*(pageNum-1)).Limit(pageCount).ToList();
            }
            client = null;

            if (query.Count > 0)
            {
                var jsonWriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.Strict };
                JArray JA = JArray.Parse(query.ToJson(jsonWriterSettings));
                foreach (JObject j in JA)
                {
                    j.Remove("_id");
                }
                return JA;
            }
            else { return new JArray(); }
        }
    }
}
