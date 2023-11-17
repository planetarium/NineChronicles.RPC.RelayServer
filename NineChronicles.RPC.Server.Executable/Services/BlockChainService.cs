using Grpc.Core;
using Grpc.Net.Client;
using MagicOnion;
using MagicOnion.Client;
using MagicOnion.Server;
using Microsoft.Extensions.Options;
using Nekoyume.Shared.Services;
using NineChronicles.RPC.Server.Executable.Options;

namespace NineChronicles.RPC.Server.Executable.Services
{
    public class BlockChainService : ServiceBase<IBlockChainService>, IBlockChainService
    {
        private readonly ILogger<BlockChainService> _logger;
        private readonly IBlockChainService _headlessService;
        private readonly IStateCache _stateCache;
        
        public BlockChainService(ILogger<BlockChainService> logger, IOptions<HeadlessConnectionOption> options, IStateCache stateCache)
        {
            HeadlessConnectionOption option = options.Value;
            var grpcChannelOptions = new GrpcChannelOptions()
            {
                Credentials = ChannelCredentials.Insecure,
                MaxReceiveMessageSize = null,
            };
            _headlessService = MagicOnionClient.Create<IBlockChainService>(GrpcChannel.ForAddress($"http://{option.Host}:{option.Port}", grpcChannelOptions));
            _stateCache = stateCache;
            _logger = logger;
        }

        public UnaryResult<bool> PutTransaction(byte[] txBytes)
        {
            _logger.LogInformation("PutTransaction");
            return new UnaryResult<bool>(Unwrap(_headlessService.PutTransaction(txBytes)));
        }

        public UnaryResult<long> GetNextTxNonce(byte[] addressBytes) =>
            new UnaryResult<long>(Unwrap(_headlessService.GetNextTxNonce(addressBytes)));

        public UnaryResult<byte[]> GetState(byte[] addressBytes, byte[] blockHashBytes)
        {
            _logger.LogDebug($"GetState: {addressBytes}, {blockHashBytes}");
            if (_stateCache.TryGetState(addressBytes, blockHashBytes, out var stateBytes))
            {
                return new UnaryResult<byte[]>(stateBytes);
            }
            var state = Unwrap(_headlessService.GetState(addressBytes, blockHashBytes));
            _stateCache.TrySetState(addressBytes, blockHashBytes, state);
            return new UnaryResult<byte[]>(state);
        }

        public UnaryResult<byte[]> GetStateBySrh(byte[] addressBytes, byte[] stateRootHashBytes)
        {
            _logger.LogDebug($"GetStateBySrh: {addressBytes}, {stateRootHashBytes}");
            if (_stateCache.TryGetStateBySrh(addressBytes, stateRootHashBytes, out var stateBytes))
            {
                return new UnaryResult<byte[]>(stateBytes);
            }
            var state = Unwrap(_headlessService.GetStateBySrh(addressBytes, stateRootHashBytes));
            _stateCache.TrySetStateBySrh(addressBytes, stateRootHashBytes, state);
            return new UnaryResult<byte[]>(state);
        }

        public UnaryResult<byte[]> GetBalance(byte[] addressBytes, byte[] currencyBytes, byte[] blockHashBytes)
        {
            if(_stateCache.TryGetBalance(addressBytes, currencyBytes, blockHashBytes, out var balanceBytes))
            {
                return new UnaryResult<byte[]>(balanceBytes);
            }
            var balance = Unwrap(_headlessService.GetBalance(addressBytes, currencyBytes, blockHashBytes));
            _stateCache.TrySetBalance(addressBytes, currencyBytes, blockHashBytes, balance);
            return new UnaryResult<byte[]>(balance);
        }

        public UnaryResult<byte[]> GetBalanceBySrh(byte[] addressBytes, byte[] currencyBytes, byte[] stateRootHashBytes)
        {
            if(_stateCache.TryGetBalanceBySrh(addressBytes, currencyBytes, stateRootHashBytes, out var balanceBytes))
            {
                return new UnaryResult<byte[]>(balanceBytes);
            }
            var balance = Unwrap(_headlessService.GetBalanceBySrh(addressBytes, currencyBytes, stateRootHashBytes));
            _stateCache.TrySetBalanceBySrh(addressBytes, currencyBytes, stateRootHashBytes, balance);
            return new UnaryResult<byte[]>(balance);
        }

        public UnaryResult<byte[]> GetTip() =>
            new UnaryResult<byte[]>(Unwrap(_headlessService.GetTip()));

        public UnaryResult<byte[]> GetBlockHash(long blockIndex) =>
            new UnaryResult<byte[]>(Unwrap(_headlessService.GetBlockHash(blockIndex)));

        public UnaryResult<bool> SetAddressesToSubscribe(byte[] toByteArray, IEnumerable<byte[]> addressesBytes) =>
            new UnaryResult<bool>(Unwrap(_headlessService.SetAddressesToSubscribe(toByteArray, addressesBytes)));

        public UnaryResult<bool> IsTransactionStaged(byte[] txidBytes) =>
            new UnaryResult<bool>(Unwrap(_headlessService.IsTransactionStaged(txidBytes))); 

        public UnaryResult<bool> ReportException(string code, string message) =>
            new UnaryResult<bool>(Unwrap(_headlessService.ReportException(code, message)));

        public UnaryResult<bool> AddClient(byte[] addressByte) =>
            new UnaryResult<bool>(Unwrap(_headlessService.AddClient(addressByte)));

        public UnaryResult<bool> RemoveClient(byte[] addressByte) =>
            new UnaryResult<bool>(Unwrap(_headlessService.RemoveClient(addressByte)));

        public UnaryResult<Dictionary<byte[], byte[]>> GetAvatarStates(IEnumerable<byte[]> addressBytesList, byte[] blockHashBytes)
        {
            var result = new Dictionary<byte[], byte[]>();
            foreach (byte[] addressBytes in addressBytesList)
            {
                if (_stateCache.TryGetState(addressBytes, blockHashBytes, out var stateBytes))
                {
                    result.Add(addressBytes, stateBytes);
                }
                else
                {
                    var state = Unwrap(_headlessService.GetState(addressBytes, blockHashBytes));
                    _stateCache.TrySetState(addressBytes, blockHashBytes, state);
                    result.Add(addressBytes, state);
                }
            }
            return new UnaryResult<Dictionary<byte[], byte[]>>(result);
        }

        public UnaryResult<Dictionary<byte[], byte[]>> GetAvatarStatesBySrh(IEnumerable<byte[]> addressBytesList, byte[] stateRootHashBytes)
        {
            var result = new Dictionary<byte[], byte[]>();
            foreach (byte[] addressBytes in addressBytesList)
            {
                if (_stateCache.TryGetStateBySrh(addressBytes, stateRootHashBytes, out var stateBytes))
                {
                    result.Add(addressBytes, stateBytes);
                }
                else
                {
                    var state = Unwrap(_headlessService.GetStateBySrh(addressBytes, stateRootHashBytes));
                    _stateCache.TrySetStateBySrh(addressBytes, stateRootHashBytes, state);
                    result.Add(addressBytes, state);
                }
            }
            return new UnaryResult<Dictionary<byte[], byte[]>>(result);
        }

        public UnaryResult<Dictionary<byte[], byte[]>> GetStateBulk(IEnumerable<byte[]> addressBytesList, byte[] blockHashBytes)
        {
            var result = new Dictionary<byte[], byte[]>();
            foreach (byte[] addressBytes in addressBytesList)
            {
                if (_stateCache.TryGetState(addressBytes, blockHashBytes, out var stateBytes))
                {
                    result.Add(addressBytes, stateBytes);
                }
                else
                {
                    var state = Unwrap(_headlessService.GetState(addressBytes, blockHashBytes));
                    _stateCache.TrySetState(addressBytes, blockHashBytes, state);
                    result.Add(addressBytes, state);
                }
            }
            return new UnaryResult<Dictionary<byte[], byte[]>>(result);
        }

        public UnaryResult<Dictionary<byte[], byte[]>> GetStateBulkBySrh(IEnumerable<byte[]> addressBytesList, byte[] stateRootHashBytes)
        {
            var result = new Dictionary<byte[], byte[]>();
            foreach (byte[] addressBytes in addressBytesList)
            {
                if (_stateCache.TryGetStateBySrh(addressBytes, stateRootHashBytes, out var stateBytes))
                {
                    result.Add(addressBytes, stateBytes);
                }
                else
                {
                    var state = Unwrap(_headlessService.GetStateBySrh(addressBytes, stateRootHashBytes));
                    _stateCache.TrySetStateBySrh(addressBytes, stateRootHashBytes, state);
                    result.Add(addressBytes, state);
                }
            }
            return new UnaryResult<Dictionary<byte[], byte[]>>(result);
        }
        
        private static T Unwrap<T>(UnaryResult<T> result)
        {
            return result.ResponseAsync.Result;
        }
    }
}