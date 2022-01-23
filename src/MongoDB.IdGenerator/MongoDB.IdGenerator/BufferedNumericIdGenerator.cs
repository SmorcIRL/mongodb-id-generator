using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SmorcIRL.MongoDB.IdGenerator
{
    public class BufferedNumericIdGenerator
    {   
        private static readonly UpdateDefinition<CustomIdentifierDocument> IncrementUpdateDefinition = Builders<CustomIdentifierDocument>.Update.Inc(x => x.HighValue, 1);
        private static readonly FindOneAndUpdateOptions<CustomIdentifierDocument> IncrementOptions = new FindOneAndUpdateOptions<CustomIdentifierDocument> { ReturnDocument = ReturnDocument.After };

        private readonly IMongoCollection<CustomIdentifierDocument> _collection;
        private readonly FilterDefinition<CustomIdentifierDocument> _findDefinition;
        private readonly SemaphoreSlim _lock;
        private readonly BufferBlock<long> _queue;

        private Task _init;
        private long _startValue;
        private int _highValue;
        private int _lowValue;
        private int _committedCount;
        private IdState[] _states;
        
        public BufferedNumericIdGenerator(IMongoCollection<CustomIdentifierDocument> collection, ObjectId id)
        {
            _collection = collection ?? throw new ArgumentNullException(nameof(collection));
            _findDefinition = Builders<CustomIdentifierDocument>.Filter.Eq(x => x.Id, id);

            _lock = new SemaphoreSlim(1);
            _queue = new BufferBlock<long>();

            _startValue = int.MinValue;
            _highValue = int.MinValue;
            _lowValue = int.MinValue;
            _states = Array.Empty<IdState>();
        }
        
        // document with Id==id must be created before Init() call
        public Task Init()
        {
            if (_init == null || _init.IsFaulted)
            {
                _init = RentRange();
                return _init;
            }
            
            throw new InvalidOperationException("Generator is already initialized");
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
                        // Ensures that even if document is corrupted or there is a network error, object will be in correct state
                        _states[index] = IdState.Free;
                        _queue.Post(value);
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
            var doc = await _collection.FindOneAndUpdateAsync
            (
                _findDefinition,
                IncrementUpdateDefinition,
                IncrementOptions
            );

            if (doc == null)
            {
                throw new InvalidOperationException("No suitable record found");
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

            var min = _startValue + (_highValue - 1) * _lowValue;

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
            return value - (_highValue - 1) * _lowValue - _startValue;
        }

        private enum IdState : byte
        {
            Free,
            Rented,
            Committed,
        }
    }
}
