using System.Net;
using Grpc.Core;
using Grpc.Net.Client;
using Libplanet.Crypto;
using MagicOnion.Client;
using Microsoft.Extensions.Options;
using Nekoyume.Shared.Hubs;
using Nekoyume.Shared.Services;
using NineChronicles.RPC.Server.Executable.Options;

namespace NineChronicles.RPC.Server.Executable.Hubs
{
    public class HeadlessHubReceiver :  IActionEvaluationHubReceiver, IHostedService, IDisposable
    {
        private readonly ChannelBase _grpcChannel;
        private readonly GrpcChannelOptions _grpcChannelOptions;
        private readonly ILogger<HeadlessHubReceiver> _logger;
        private readonly PrivateKey _privateKey = new PrivateKey();
        private IActionEvaluationHub? _headlessHub;
        private IActionEvaluationHub? _hub;
        
        public HeadlessHubReceiver(ILogger<HeadlessHubReceiver> logger, IOptions<HeadlessConnectionOption> options)
        {
            HeadlessConnectionOption option = options.Value;
            _grpcChannelOptions = new GrpcChannelOptions()
            {
                Credentials = ChannelCredentials.Insecure,
                MaxReceiveMessageSize = null,
            };
            _grpcChannel = GrpcChannel.ForAddress($"http://{option.Host}:{option.Port}", _grpcChannelOptions);
            _logger = logger;
        }
        
        public IBlockChainService GetHeadlessService()
        {
            if (BlockChainService is null)
            {
                throw new Exception("HeadlessService is null.");
            }
            return BlockChainService;
        }

        public IBlockChainService? BlockChainService
        {
            get;
            private set;
        }

        public void OnRender(byte[] evaluation)
        {
            _hub?.BroadcastRenderAsync(evaluation);
        }

        public void OnUnrender(byte[] evaluation)
        {
            _hub?.BroadcastUnrenderAsync(evaluation);
        }

        public void OnRenderBlock(byte[] oldTip, byte[] newTip)
        {
            _hub?.BroadcastRenderBlockAsync(oldTip, newTip);
        }

        public void OnReorged(byte[] oldTip, byte[] newTip, byte[] branchpoint)
        {
            _hub?.ReportReorgAsync(oldTip, newTip, branchpoint);
        }

        public void OnReorgEnd(byte[] oldTip, byte[] newTip, byte[] branchpoint)
        {
            _hub?.ReportReorgEndAsync(oldTip, newTip, branchpoint);
        }

        public void OnException(int code, string message)
        {
            _hub?.ReportExceptionAsync(code, message);
        }

        public void OnPreloadStart()
        {
            _hub?.PreloadStartAsync();
        }

        public void OnPreloadEnd()
        {
            _hub?.PreloadEndAsync();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _headlessHub = await StreamingHubClient.ConnectAsync<IActionEvaluationHub, IActionEvaluationHubReceiver>(
                _grpcChannel, 
                this,
                cancellationToken: cancellationToken);

            BlockChainService = MagicOnionClient.Create<IBlockChainService>(_grpcChannel);
            await _headlessHub.JoinAsync(_privateKey.ToAddress().ToHex());
            await BlockChainService.AddClient(_privateKey.ToAddress().ToByteArray());
            await BlockChainService.SetAddressesToSubscribe(_privateKey.ToAddress().ToByteArray(), new []
            {
                _privateKey.ToAddress().ToByteArray(),
            });
            _logger.LogInformation("Connected to HeadlessHub");
            
            var channel = GrpcChannel.ForAddress($"http://{IPAddress.Loopback.ToString()}:5250", _grpcChannelOptions); 
            _hub = StreamingHubClient.ConnectAsync<IActionEvaluationHub, IActionEvaluationHubReceiver>(
                channel,
                null!
            ).Result;
            _hub?.JoinAsync(_privateKey.ToAddress().ToHex());
            _logger.LogInformation("Connected to ActionEvaluationHub");
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _headlessHub?.LeaveAsync();
            _hub?.LeaveAsync();
            _logger.LogInformation("Disconnected from ActionEvaluationHub.");
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _headlessHub?.DisposeAsync();
            _hub?.DisposeAsync();
        }
    }
}