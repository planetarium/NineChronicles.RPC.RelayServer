using Libplanet.Crypto;
using LruCacheNet;

namespace NineChronicles.RPC.Server.Executable
{
    public class StateCache : IStateCache
    {
        private readonly ILogger<StateCache> _logger;
        private readonly LruCache<(byte[], byte[]), byte[]> _stateByBlockCache;
        private readonly LruCache<(byte[], byte[]), byte[]> _stateBySrhCache;
        private readonly LruCache<(byte[], byte[], byte[]), byte[]> _balanceByBlockCache;
        private readonly LruCache<(byte[], byte[], byte[]), byte[]> _balanceBySrhCache;
        private readonly PrivateKey _privateKey = new PrivateKey();

        public StateCache(ILogger<StateCache> logger)
        {
            _logger = logger;
            _stateByBlockCache = new LruCache<(byte[], byte[]), byte[]>(capacity: 50000);
            _stateBySrhCache = new LruCache<(byte[], byte[]), byte[]>(capacity: 100000);
            _balanceByBlockCache = new LruCache<(byte[], byte[], byte[]), byte[]>(capacity: 50000);
            _balanceBySrhCache = new LruCache<(byte[], byte[], byte[]), byte[]>(capacity: 100000);
        }

        public bool TryGetState(byte[] addressBytes, byte[] blockHashBytes, out byte[] stateBytes)
        {
            _logger.LogInformation($"StateCache called: {_privateKey.ToAddress()}");
            var result = _stateByBlockCache.TryGetValue((addressBytes, blockHashBytes), out stateBytes);
            if (result)
            {
                _logger.LogInformation($"Cache hit: TryGetState Cache size: {_stateByBlockCache.Count}");
            }
            return result;
        }

        public bool TrySetState(byte[] addressBytes, byte[] blockHashBytes, byte[] stateBytes) =>
            _stateByBlockCache.TryAdd((addressBytes, blockHashBytes), stateBytes);

        public bool TryGetStateBySrh(byte[] addressBytes, byte[] stateRootHashBytes, out byte[] stateBytes)
        {
            var result = _stateBySrhCache.TryGetValue((addressBytes, stateRootHashBytes), out stateBytes);
            if (result)
            {
                _logger.LogDebug($"Cache hit: TryGetStateBySrh Cache size: {_stateBySrhCache.Count}");
            }
            return result;
        }

        public bool TrySetStateBySrh(byte[] addressBytes, byte[] stateRootHashBytes, byte[] stateBytes) =>
            _stateBySrhCache.TryAdd((addressBytes, stateRootHashBytes), stateBytes);

        public bool TryGetBalance(byte[] addressBytes, byte[] currencyBytes, byte[] blockHashBytes, out byte[] stateBytes)
        {
            var result = _balanceByBlockCache.TryGetValue((addressBytes, currencyBytes, blockHashBytes), out stateBytes);
            if (result)
            {
                _logger.LogDebug($"Cache hit: TryGetBalance Cache size: {_balanceByBlockCache.Count}");
            }
            return result;
        }

        public bool TrySetBalance(byte[] addressBytes, byte[] currencyBytes, byte[] blockHashBytes, byte[] balanceBytes) =>
            _balanceByBlockCache.TryAdd((addressBytes, currencyBytes, blockHashBytes), balanceBytes);

        public bool TryGetBalanceBySrh(byte[] addressBytes, byte[] currencyBytes, byte[] stateRootHashBytes, out byte[] stateBytes)
        {
            var result = _balanceBySrhCache.TryGetValue((addressBytes, currencyBytes, stateRootHashBytes), out stateBytes);
            if (result)
            {
                _logger.LogDebug($"Cache hit: TryGetBalanceBySrh Cache size: {_balanceBySrhCache.Count}");
            }
            return result;
        }

        public bool TrySetBalanceBySrh(byte[] addressBytes, byte[] currencyBytes, byte[] stateRootHashBytes, byte[] balanceBytes) =>
            _balanceBySrhCache.TryAdd((addressBytes, currencyBytes, stateRootHashBytes), balanceBytes);
    }
}