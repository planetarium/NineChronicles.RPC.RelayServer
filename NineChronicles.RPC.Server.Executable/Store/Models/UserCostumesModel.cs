namespace NineChronicles.RPC.Server.Store.Models
{
    using System;

    public class UserCostumesModel
    {
        public long? BlockIndex { get; set; }

        public string? ItemId { get; set; }

        public string? AgentAddress { get; set; }

        public AgentModel? Agent { get; set; }

        public string? AvatarAddress { get; set; }

        public AvatarModel? Avatar { get; set; }

        public string? ItemType { get; set; }

        public string? ItemSubType { get; set; }

        public int? Id { get; set; }

        public string? ElementalType { get; set; }

        public int? Grade { get; set; }

        public bool? Equipped { get; set; }

        public string? SpineResourcePath { get; set; }

        public long? RequiredBlockIndex { get; set; }

        public string? NonFungibleId { get; set; }

        public string? TradableId { get; set; }

        public DateTimeOffset? TimeStamp { get; set; }
    }
}
