﻿namespace NineChronicles.RPC.Server.Store.Models
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using Microsoft.EntityFrameworkCore;

    [Index(nameof(Date))]

    public class TransactionModel
    {
        public long BlockIndex { get; set; }

        public string? BlockHash { get; set; }

        [Key]
        public string? TxId { get; set; }

        public string? Signer { get; set; }

        public string? ActionType { get; set; }

        public long Nonce { get; set; }

        public string? PublicKey { get; set; }

        public int? UpdatedAddressesCount { get; set; }

        public DateTimeOffset Date { get; set; }

        public DateTimeOffset TimeStamp { get; set; }
    }
}
