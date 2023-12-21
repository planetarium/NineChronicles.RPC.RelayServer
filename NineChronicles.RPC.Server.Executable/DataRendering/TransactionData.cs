using Libplanet.Action.Loader;
using Nekoyume.Action.Loader;

namespace NineChronicles.RPC.Server.DataRendering
{
    using System.Collections.Generic;
    using System.Linq;
    using Bencodex.Types;
    using Libplanet.Action;
    using Libplanet.Types.Blocks;
    using Libplanet.Types.Tx;
    using Nekoyume.Action;
    using NineChronicles.RPC.Server.Store.Models;

    public static class TransactionData
    {
        public static TransactionModel GetTransactionInfo(
            Block block,
            Transaction transaction
        )
        {
            IActionLoader actionLoader = new NCActionLoader();
            var actionType = actionLoader.LoadAction(0, transaction.Actions.FirstOrDefault()!)
                .ToString()!.Split('.').LastOrDefault()!.Replace(">", string.Empty);
            var transactionModel = new TransactionModel
            {
                BlockIndex = block.Index,
                BlockHash = block.Hash.ToString(),
                TxId = transaction.Id.ToString(),
                Signer = transaction.Signer.ToString(),
                ActionType = actionType,
                Nonce = transaction.Nonce,
                PublicKey = transaction.PublicKey.ToString(),
                UpdatedAddressesCount = transaction.UpdatedAddresses.Count(),
                Date = transaction.Timestamp.UtcDateTime,
                TimeStamp = transaction.Timestamp.UtcDateTime,
            };

            return transactionModel;
        }
    }
}
