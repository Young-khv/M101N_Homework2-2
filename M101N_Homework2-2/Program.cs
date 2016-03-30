using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace M101N_Homework2_2
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
            Console.ReadKey();
        }

        private static async Task MainAsync(string[] args)
        {
            const string conString = "mongodb://localhost:27017";

            var client = new MongoClient(conString);

            var db = client.GetDatabase("students");

            var collection = db.GetCollection<BsonDocument>("grades");

            var builder = Builders<BsonDocument>.Filter;

            var sort = Builders<BsonDocument>.Sort.Ascending("student_id").Ascending("score");

            var filter = builder.Eq("type", "homework");
            
            var result = await collection.Find(filter)
                .Sort(sort)
                .ToListAsync();
            var st_id = -1;
            foreach (var student in result)
            {
                if (student["student_id"] != st_id)
                {
                    st_id = (int)student["student_id"];
                    await collection.DeleteOneAsync(student);
                    Console.WriteLine(st_id);
                }
            }
        }
    }
}
