using Libplanet.Crypto;
using MagicOnion.Server.Hubs;
using Nekoyume.Shared.Hubs;
using Nekoyume.Shared.Services;

namespace NineChronicles.RPC.Server.Executable.Hubs
{
    public class ActionEvaluationHub : StreamingHubBase<IActionEvaluationHub, IActionEvaluationHubReceiver>, IActionEvaluationHub
    {
        private readonly ILogger<ActionEvaluationHub> _logger;
        private readonly PrivateKey _privateKey = new PrivateKey();

        private IGroup _addressGroup;
        
        public ActionEvaluationHub(ILogger<ActionEvaluationHub> logger)
        {
            _logger = logger;
        }

        public async Task JoinAsync(string addressHex)
        {
            _addressGroup = await Group.AddAsync("UnityClient");
            _logger.LogInformation(
                $"Client {addressHex} joined. Group: {_addressGroup.GroupName} MemberCount: {_addressGroup.GetMemberCountAsync().Result.ToString()}");
        }

        public async Task LeaveAsync()
        {
            await _addressGroup.RemoveAsync(Context);
            _logger.LogDebug($"Client {_privateKey.ToAddress().ToHex()} left.");
        }
        public async Task BroadcastRenderAsync(byte[] outputStates)
        {
            Broadcast(_addressGroup).OnRender(outputStates);
            await Task.CompletedTask;
        }

        public async Task BroadcastUnrenderAsync(byte[] outputStates)
        {
            Broadcast(_addressGroup).OnUnrender(outputStates);
            await Task.CompletedTask;
        }

        public async Task BroadcastRenderBlockAsync(byte[] oldTip, byte[] newTip)
        {
            _logger.LogInformation($"RenderBlock Thread: {_privateKey.ToAddress()}");
            Broadcast(_addressGroup).OnRenderBlock(oldTip, newTip);
            await Task.CompletedTask;
        }

        public async Task ReportReorgAsync(byte[] oldTip, byte[] newTip, byte[] branchpoint)
        {
            Broadcast(_addressGroup).OnReorged(oldTip, newTip, branchpoint);
            _logger.LogDebug($"ReportReorgAsync: {oldTip}, {newTip}, {branchpoint}");
            await Task.CompletedTask;
        }

        public async Task ReportReorgEndAsync(byte[] oldTip, byte[] newTip, byte[] branchpoint)
        {
            Broadcast(_addressGroup).OnReorgEnd(oldTip, newTip, branchpoint);
            _logger.LogDebug($"ReportReorgEndAsync: {oldTip}, {newTip}, {branchpoint}");
            await Task.CompletedTask;
        }

        public async Task ReportExceptionAsync(int code, string message)
        {
            Broadcast(_addressGroup).OnException(code, message);
            _logger.LogDebug($"ReportExceptionAsync: {code}, {message}");
            await Task.CompletedTask;
        }

        public async Task PreloadStartAsync()
        {
            Broadcast(_addressGroup).OnPreloadStart();
            await Task.CompletedTask;
        }

        public async Task PreloadEndAsync()
        {
            Broadcast(_addressGroup).OnPreloadEnd();
            await Task.CompletedTask;
        }
    }
}