namespace NineChronicles.RPC.Server.DataRendering
{
    using Nekoyume.Model.State;
    using NineChronicles.RPC.Server.Store.Models;

    public static class RaidData
    {
        public static RaiderModel GetRaidInfo(
            int raidId,
            RaiderState raiderState
        )
        {
            var raiderModel = new RaiderModel(
                raidId,
                raiderState.AvatarName,
                raiderState.HighScore,
                raiderState.TotalScore,
                raiderState.Cp,
                raiderState.IconId,
                raiderState.Level,
                raiderState.AvatarAddress.ToHex(),
                raiderState.PurchaseCount);

            return raiderModel;
        }
    }
}
