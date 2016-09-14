using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AkkaStreams.Wiki
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(BuildMaterializedView().GetAwaiter().GetResult().ToString());
            Console.ReadKey();
        }

        static async Task<TotalsByCategoryView> BuildMaterializedView()
        {
            //build mock stream source of SourcePoco objects
            //just pretend that these come from the DB as an IEnumerable
            var list = new List<SourcePoco>();
            var category = "testcat";
            for(int i=0;i<50;i++)
            {
                if(i % 10 == 0)
                {
                    category = "testcat" + i;
                }
                var s = new SourcePoco(i, category, 10.0m);
                list.Add(s);
                //Console.WriteLine(s);
            }
            //now we have our mock stream of pocos
            using (var system = ActorSystem.Create("streams-example"))
            using (var materializer = system.Materializer())
            {
                //create actor system and materializer
                //so this takes our source and Runs an Aggregation lambda for every item enumerated
                //it is seeded with a brand new instance of the TotalsByCategoryView
                //each item is handed to the view and the view calls its Mutate method to 'apply' the changes
                //needed for the item passed in
                //technically we COULD load up the TotalsByCategoryView from the persistence store (SQL Server or Cassandra or Redis or S3 or ....)
                //apply these changes to it, then when it returns, write the updated version of the view back out
                return await Source
                    .From(list)
                    .RunWith(Sink.Aggregate<SourcePoco, TotalsByCategoryView>(new TotalsByCategoryView(),(v,s) =>
                     {
                         return v.Mutate(s);
                     }), materializer);
                    
            }
        }

        #region other silly samples

        static async Task Run()
        {
            var text = @"
                Lorem Ipsum is simply dummy text of the printing and typesetting industry.
                Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
                when an unknown printer took a galley of type and scrambled it to make a type
                specimen book.";

            using (var system = ActorSystem.Create("streams-example"))
            using (var materializer = system.Materializer())
            {
                await Source
                    .From(text.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries))
                    .Select(t => t.Trim())
                    .RunForeach(Console.WriteLine, materializer);
            }
        }

        static async Task<int> Adder()
        {
            var text = @"1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19";

            using (var system = ActorSystem.Create("streams-example"))
            using (var materializer = system.Materializer())
            {
                return await Source
                    .From(text.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries))
                    .Select(t => int.Parse(t.Trim()))
                    .RunSum((a, b) =>
                    {
                        return a + b;
                    }, materializer);
            }
        }
        #endregion
    }

    #region Sample POCO and TotalsByCategoryView

    public class SourcePoco
    {
        public long Id { get; set; }
        public string Category { get; set; }
        public decimal Amount { get; set; }

        public SourcePoco(long id, string cat, decimal amount)
        {
            Id = id;
            Category = cat;
            Amount = amount;
        }

        public override string ToString()
        {
            return string.Format("Source Poco: {0} Cat={1} Amount={1}",Id, Category, Amount);
        }
    }
    public class TotalsByCategoryView
    {
        Dictionary<string, decimal> _totalsByCategory;

        public TotalsByCategoryView()
        {
            _totalsByCategory = new Dictionary<string, decimal>();
        }

        public TotalsByCategoryView Mutate(SourcePoco s)
        {
            if (this._totalsByCategory.ContainsKey(s.Category))
            {
                _totalsByCategory[s.Category] += s.Amount;
            }
            else
            {
                _totalsByCategory.Add(s.Category, s.Amount);
            }

            return this;
        }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this._totalsByCategory,Formatting.Indented);
        }
    }

    #endregion
}
