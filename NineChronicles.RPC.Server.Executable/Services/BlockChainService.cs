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

        public UnaryResult<byte[]> GetStateByBlockHash(byte[] blockHashBytes, byte[] accountAddressBytes, byte[] addressBytes) =>
            new UnaryResult<byte[]>(Unwrap(_headlessService.GetStateByBlockHash(blockHashBytes, accountAddressBytes, addressBytes)));

        public UnaryResult<byte[]> GetStateByStateRootHash(byte[] stateRootHashBytes, byte[] accountAddressBytes, byte[] addressBytes) =>
            new UnaryResult<byte[]>(Unwrap(_headlessService.GetStateByStateRootHash(stateRootHashBytes, accountAddressBytes, addressBytes)));

        public UnaryResult<byte[]> GetBalanceByBlockHash(byte[] blockHashBytes, byte[] addressBytes, byte[] currencyBytes) =>
            new UnaryResult<byte[]>(Unwrap(_headlessService.GetBalanceByBlockHash(blockHashBytes, addressBytes, currencyBytes)));

        public UnaryResult<byte[]> GetBalanceByStateRootHash(byte[] stateRootHashBytes, byte[] addressBytes, byte[] currencyBytes) =>
            new UnaryResult<byte[]>(Unwrap(_headlessService.GetBalanceByStateRootHash(stateRootHashBytes, addressBytes, currencyBytes)));

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

        public UnaryResult<Dictionary<byte[], byte[]>> GetAgentStatesByBlockHash(byte[] blockHashBytes, IEnumerable<byte[]> addressBytesList) =>
            new UnaryResult<Dictionary<byte[], byte[]>>(Unwrap(_headlessService.GetAgentStatesByBlockHash(blockHashBytes, addressBytesList)));

        public UnaryResult<Dictionary<byte[], byte[]>> GetAgentStatesByStateRootHash(byte[] stateRootHashBytes, IEnumerable<byte[]> addressBytesList) =>
            new UnaryResult<Dictionary<byte[], byte[]>>(Unwrap(_headlessService.GetAgentStatesByStateRootHash(stateRootHashBytes, addressBytesList)));

        public UnaryResult<Dictionary<byte[], byte[]>> GetAvatarStatesByBlockHash(byte[] blockHashBytes, IEnumerable<byte[]> addressBytesList) =>
            new UnaryResult<Dictionary<byte[], byte[]>>(Unwrap(_headlessService.GetAvatarStatesByBlockHash(blockHashBytes, addressBytesList)));

        public UnaryResult<Dictionary<byte[], byte[]>> GetAvatarStatesByStateRootHash(byte[] stateRootHashBytes, IEnumerable<byte[]> addressBytesList) =>
            new UnaryResult<Dictionary<byte[], byte[]>>(Unwrap(_headlessService.GetAvatarStatesByStateRootHash(stateRootHashBytes, addressBytesList)));

        public UnaryResult<Dictionary<byte[], byte[]>> GetBulkStateByBlockHash(byte[] blockHashBytes, byte[] accountAddressBytes, IEnumerable<byte[]> addressBytesList) =>
        new UnaryResult<Dictionary<byte[], byte[]>>(Unwrap(_headlessService.GetBulkStateByBlockHash(blockHashBytes, accountAddressBytes, addressBytesList)));
        
        public UnaryResult<Dictionary<byte[], byte[]>> GetBulkStateByStateRootHash(byte[] stateRootHashBytes, byte[] accountAddressBytes, IEnumerable<byte[]> addressBytesList) =>
        new UnaryResult<Dictionary<byte[], byte[]>>(Unwrap(_headlessService.GetBulkStateByStateRootHash(stateRootHashBytes, accountAddressBytes, addressBytesList)));

        public UnaryResult<Dictionary<byte[], byte[]>> GetSheets(byte[] blockHashBytes, IEnumerable<byte[]> addressBytesList) =>
            new UnaryResult<Dictionary<byte[], byte[]>>(Unwrap(_headlessService.GetSheets(blockHashBytes, addressBytesList)));

        private static T Unwrap<T>(UnaryResult<T> result)
        {
            return result.ResponseAsync.Result;
        }
    }
}