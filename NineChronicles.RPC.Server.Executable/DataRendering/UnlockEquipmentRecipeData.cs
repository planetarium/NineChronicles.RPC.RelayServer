﻿namespace NineChronicles.RPC.Server.DataRendering
{
    using System;
    using System.Collections.Generic;
    using Libplanet;
    using Libplanet.Action;
    using Libplanet.Action.State;
    using Libplanet.Crypto;
    using Libplanet.Types.Assets;
    using Nekoyume.Helper;
    using NineChronicles.RPC.Server.Store.Models;

    public static class UnlockEquipmentRecipeData
    {
        public static List<UnlockEquipmentRecipeModel> GetUnlockEquipmentRecipeInfo(
            IAccount previousStates,
            IAccount outputStates,
            Address signer,
            Address avatarAddress,
            List<int> recipeIds,
            Guid actionId,
            long blockIndex,
            DateTimeOffset blockTime
        )
        {
            Currency crystalCurrency = CrystalCalculator.CRYSTAL;
            var prevCrystalBalance = previousStates.GetBalance(
                signer,
                crystalCurrency);
            var outputCrystalBalance = outputStates.GetBalance(
                signer,
                crystalCurrency);
            var burntCrystal = prevCrystalBalance - outputCrystalBalance;
            var unlockEquipmentRecipeList = new List<UnlockEquipmentRecipeModel>();
            foreach (var recipeId in recipeIds)
            {
                unlockEquipmentRecipeList.Add(new UnlockEquipmentRecipeModel()
                {
                    Id = actionId.ToString(),
                    BlockIndex = blockIndex,
                    AgentAddress = signer.ToString(),
                    AvatarAddress = avatarAddress.ToString(),
                    UnlockEquipmentRecipeId = recipeId,
                    BurntCrystal = Convert.ToDecimal(burntCrystal.GetQuantityString()),
                    Date = DateOnly.FromDateTime(blockTime.DateTime),
                    TimeStamp = blockTime,
                });
            }

            return unlockEquipmentRecipeList;
        }
    }
}
