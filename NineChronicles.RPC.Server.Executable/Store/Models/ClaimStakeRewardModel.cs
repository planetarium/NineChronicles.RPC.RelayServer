namespace NineChronicles.RPC.Server.Store.Models
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using Microsoft.EntityFrameworkCore;

    [Index(nameof(Date))]

    public class ClaimStakeRewardModel
    {
        [Key]
        public string? Id { get; set; }

        public long BlockIndex { get; set; }

        public string? AgentAddress { get; set; }

        public AgentModel? Agent { get; set; }

        public string? ClaimRewardAvatarAddress { get; set; }

        public int HourGlassCount { get; set; }

        public int ApPotionCount { get; set; }

        public long ClaimStakeStartBlockIndex { get; set; }

        public long ClaimStakeEndBlockIndex { get; set; }

        public DateOnly Date { get; set; }

        public DateTimeOffset TimeStamp { get; set; }
    }
}
