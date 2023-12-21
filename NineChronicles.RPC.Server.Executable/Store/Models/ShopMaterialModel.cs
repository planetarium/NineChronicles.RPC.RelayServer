namespace NineChronicles.RPC.Server.Store.Models
{
    using System;
    using System.ComponentModel.DataAnnotations;

    public class ShopMaterialModel
    {
        public long BlockIndex { get; set; }

        [Key]
        public string? ItemId { get; set; }

        public string? SellerAgentAddress { get; set; }

        public string? SellerAvatarAddress { get; set; }

        public string? ItemType { get; set; }

        public string? ItemSubType { get; set; }

        public int Id { get; set; }

        public string? ElementalType { get; set; }

        public int Grade { get; set; }

        public string? TradableId { get; set; }

        public decimal Price { get; set; }

        public string? OrderId { get; set; }

        public int CombatPoint { get; set; }

        public int ItemCount { get; set; }

        public long SellStartedBlockIndex { get; set; }

        public long SellExpiredBlockIndex { get; set; }

        public DateTimeOffset TimeStamp { get; set; }
    }
}
