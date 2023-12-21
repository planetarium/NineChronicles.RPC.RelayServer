namespace NineChronicles.RPC.Server.DataRendering
{
    using Libplanet;
    using Libplanet.Crypto;
    using NineChronicles.RPC.Server.Store.Models;

    public static class AgentData
    {
        public static AgentModel GetAgentInfo(Address address)
        {
            var agentModel = new AgentModel
            {
                Address = address.ToString(),
            };

            return agentModel;
        }
    }
}
