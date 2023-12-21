﻿namespace NineChronicles.RPC.Server.Store.Models
{
    using System.ComponentModel.DataAnnotations;

    public class CraftRankingModel
    {
        [Key]
        public string? AvatarAddress { get; set; }

        public string? AgentAddress { get; set; }

        public int CraftCount { get; set; }

        public long BlockIndex { get; set; }

        public string? Name { get; set; }

        public int? AvatarLevel { get; set; }

        public int? TitleId { get; set; }

        public int? ArmorId { get; set; }

        public int? Cp { get; set; }

        public int Ranking { get; set; }
    }
}
