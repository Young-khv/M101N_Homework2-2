using System;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace M101N_Homework3_1
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
            Console.WriteLine("Done");
            Console.ReadKey();
        }

        private static async Task MainAsync(string[] args)
        {
            const string conString = "mongodb://localhost:27017";

            var client = new MongoClient(conString);

            var db = client.GetDatabase("school");

            var collection = db.GetCollection<BsonDocument>("students");

            var builder = Builders<BsonDocument>.Filter;

            var filter = new BsonDocument();
            using (var cursor = await collection.FindAsync(filter))
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    var currId = -1;
                    var currMin = -100.0;
                    foreach (var document in batch)
                    {
                        var id = (int)document["_id"];
                        var scores = document["scores"];

                        foreach (var element in scores.AsBsonArray.Where(t=>t["type"] == "homework"))
                        {
                            var score = (double)element["score"];
                            if (currId != id)
                            {
                                currMin = score;
                                currId = id;
                            }
                            else
                            {
                                if (currMin > score)
                                    currMin = score;
                               await DeleteItemFromCollection(collection, id, currMin);
                            }
                        }
                    }
                }
            }

        }

        private static async Task DeleteItemFromCollection(IMongoCollection<BsonDocument> collection, int id, double score)
        {
            var filter = new BsonDocument("_id", id);
            var update = Builders<BsonDocument>.Update.PullFilter("scores",
               Builders<BsonDocument>.Filter.And(
                         Builders<BsonDocument>.Filter.Eq("type", "homework"),
                         Builders<BsonDocument>.Filter.Eq("score", score)
               ));
           await collection.FindOneAndUpdateAsync(filter, update);
        }
    }
}
