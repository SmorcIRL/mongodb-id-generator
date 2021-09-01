using System;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace SmorcIRL.MongoDB.IdGenerator
{
    public class SimpleIncrementIdGenerator
    {
        private const long DefaultStartValue = 1;
        private const int DefaultHighValue = -1;

        private static readonly FindOneAndUpdateOptions<IdSyncDocument> IncrementOptions = new FindOneAndUpdateOptions<IdSyncDocument>
        {
            ReturnDocument = ReturnDocument.After,
        };
        private static readonly UpdateDefinition<IdSyncDocument> IncrementDefinition = Builders<IdSyncDocument>.Update.Inc(x => x.HighValue, 1);

        private readonly IMongoClient _client;
        private readonly IMongoCollection<IdSyncDocument> _collection;
        private readonly string _tag;

        public SimpleIncrementIdGenerator(IMongoClient client, IMongoCollection<IdSyncDocument> collection, string tag)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _collection = collection ?? throw new ArgumentNullException(nameof(collection));
            _tag = tag ?? throw new ArgumentNullException(nameof(tag));
        }

        public async Task Init()
        {
            using (var session = await _client.StartSessionAsync())
            {
                await session.WithTransactionAsync
                (
                    async (s, t) =>
                    {
                        var doc = await 
                        (
                            await _collection.FindAsync(s, x => x.Tag == _tag, cancellationToken: t)
                        ).FirstOrDefaultAsync(t);

                        if (doc == null)
                        {
                            await _collection.InsertOneAsync
                            (
                                s,
                                new IdSyncDocument
                                {
                                    Tag = _tag,
                                    StartValue = DefaultStartValue,
                                    HighValue = DefaultHighValue,
                                    LowValue = 1,
                                },
                                cancellationToken: t
                            );
                        }
                        else if (doc.LowValue != 1)
                        {
                            throw new InvalidOperationException();
                        }

                        return Task.CompletedTask;
                    }
                );
            }
        }

        public async Task<long> GetNext()
        {
            IdSyncDocument doc;

            using (var session = await _client.StartSessionAsync())
            {
                doc = await session.WithTransactionAsync
                (
                    (s, t) => _collection.FindOneAndUpdateAsync
                    (
                        s,
                        Builders<IdSyncDocument>.Filter.Eq(x => x.Tag, _tag),
                        IncrementDefinition,
                        IncrementOptions,
                        t
                    )
                );
            }
            
            if (doc.LowValue != 1)
            {
                throw new InvalidOperationException();
            }
            
            return doc.HighValue;
        }
    }
}