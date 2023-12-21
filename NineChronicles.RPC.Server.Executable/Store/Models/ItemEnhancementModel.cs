namespace NineChronicles.RPC.Server.Store.Models
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using Microsoft.EntityFrameworkCore;

    [Index(nameof(Date))]

    public class ItemEnhancementModel
    {
        [Key]
        public string? Id { get; set; }

        public string? AvatarAddress { get; set; }

        public AvatarModel? Avatar { get; set; }

        public string? AgentAddress { get; set; }

        public AgentModel? Agent { get; set; }

        public string? ItemId { get; set; }

        public string? MaterialId { get; set; }

        public int MaterialIdsCount { get; set; }

        public int SlotIndex { get; set; }

        public decimal BurntNCG { get; set; }

        public long BlockIndex { get; set; }

        public int? SheetId { get; set; }

        public int? Level { get; set; }

        public long? Exp { get; set; }

        public DateOnly Date { get; set; }

        public DateTimeOffset TimeStamp { get; set; }
    }
}
