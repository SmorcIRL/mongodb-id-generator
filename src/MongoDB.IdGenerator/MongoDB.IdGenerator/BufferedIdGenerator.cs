using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MongoDB.Driver;

namespace SmorcIRL.MongoDB.IdGenerator
{
    public class BufferedIdGenerator
    {
        private const long DefaultStartValue = 1;
        // After the first increment it'll be 0, so the first rented range will be [StartValue, StartValue + LowValue - 1]
        private const int DefaultHighValue = -1;
        // It's possible to change this value during runtime, but that requires recalculation of HighValue in case of decreasing
        // Also such changing may cause ids leaks so it shouldn't happen too often
        private const int DefaultLowValue = 20;

        private static readonly FindOneAndUpdateOptions<IdSyncDocument> IncrementOptions = new FindOneAndUpdateOptions<IdSyncDocument>
        {
            ReturnDocument = ReturnDocument.After,
        };
        private static readonly UpdateDefinition<IdSyncDocument> IncrementDefinition = Builders<IdSyncDocument>.Update.Inc(x => x.HighValue, 1);

        private readonly IMongoClient _client;
        private readonly IMongoCollection<IdSyncDocument> _collection;
        
        private CancellationTokenSource _rentingCancellation;
        private readonly SemaphoreSlim _lock;
        private readonly BufferBlock<long> _queue;
        private readonly string _tag;

        private long _startValue;
        private int _highValue;
        private int _lowValue;
        private int _committedCount;
        private IdState[] _states;
        
        public BufferedIdGenerator(IMongoClient client, IMongoCollection<IdSyncDocument> collection, string tag)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _collection = collection ?? throw new ArgumentNullException(nameof(collection));
            _tag = tag ?? throw new ArgumentNullException(nameof(tag));

            _rentingCancellation = new CancellationTokenSource();
            _lock = new SemaphoreSlim(1);
            _queue = new BufferBlock<long>();

            _startValue = int.MinValue;
            _highValue = int.MinValue;
            _lowValue = int.MinValue;
            _states = Array.Empty<IdState>();
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
                                    LowValue = DefaultLowValue,
                                },
                                cancellationToken: t
                            );
                        }

                        return Task.CompletedTask;
                    }
                );
            }

            await RentRange();
        }

        public async Task<long> Rent()
        {
            var rented = await _queue.ReceiveAsync();

            await _lock.WaitAsync();

            try
            {
                var index = GetIndexByValue(rented);

                _states[index] = IdState.Rented;

                return rented;
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Commit(long value)
        {
            await _lock.WaitAsync();

            try
            {
                var index = GetIndexEnsureRented(value);

                _states[index] = IdState.Committed;
                _committedCount++;

                if (_committedCount == _lowValue)
                {
                    try
                    {
                        await RentRange();
                    }
                    catch
                    {
                        // Ensures that even if sync doc is corrupted or mongo feels bad, object will be in correct state
                        _states[index] = IdState.Rented;
                        _committedCount--;
                        
                        throw;
                    }
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Release(long value)
        {
            await _lock.WaitAsync();

            try
            {
                var index = GetIndexEnsureRented(value);

                _states[index] = IdState.Free;

                _queue.Post(value);
            }
            finally
            {
                _lock.Release();
            }
        }

        private async Task RentRange()
        {
            IdSyncDocument doc;

            using (var session = await _client.StartSessionAsync())
            {
                // Ensures that we get an actual incremented HighValue, even if write conflict happens on update
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
            
            // Some checks to ensure that no range overlapping will happen
            
            if (doc.HighValue <= _highValue)
            {
                throw new InvalidOperationException();
            }
            
            if (doc.LowValue < _lowValue)
            {
                throw new InvalidOperationException();
            }

            if (doc.StartValue < _startValue)
            {
                throw new InvalidOperationException();
            }

            _highValue = doc.HighValue;
            _lowValue = doc.LowValue;
            _startValue = doc.StartValue;
            _committedCount = 0;

            if (_states.Length != _lowValue)
            {
                _states = new IdState[_lowValue];
            }

            var min = _startValue + _highValue * _lowValue;

            for (var i = 0; i < _lowValue; i++)
            {
                _states[i] = IdState.Free;
                _queue.Post(min + i);
            }
        }

        private long GetIndexEnsureRented(long value)
        {
            var index = GetIndexByValue(value);

            if (index < 0 || index >= _lowValue)
            {
                throw new ArgumentOutOfRangeException(nameof(value));
            }

            if (_states[index] != IdState.Rented)
            {
                throw new InvalidOperationException();
            }

            return index;
        }

        private long GetIndexByValue(long value)
        {
            return value - _lowValue * _highValue - _startValue;
        }

        private enum IdState : byte
        {
            Free,
            Rented,
            Committed,
        }
    }
}