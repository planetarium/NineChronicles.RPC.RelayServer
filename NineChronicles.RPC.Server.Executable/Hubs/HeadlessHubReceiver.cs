using System.Diagnostics;
using System.IO.Compression;
using System.Net;
using System.Text;
using Bencodex;
using Bencodex.Types;
using Grpc.Core;
using Grpc.Net.Client;
using Lib9c.Renderers;
using Libplanet.Crypto;
using Libplanet.Common;
using MagicOnion.Client;
using Microsoft.Extensions.Options;
using Nekoyume.Shared.Hubs;
using Nekoyume.Shared.Services;
using Nekoyume.Action;
using NineChronicles.RPC.Server.Executable.Options;
using Libplanet.Types.Blocks;
using MessagePack;
using Nekoyume.Action.Loader;
using Bencodex;
using Bencodex.Types;
using Lib9c.Formatters;
using Lib9c.Model.Order;
using Libplanet.Action.State;
using Libplanet.Types.Tx;
using MessagePack.Resolvers;
using Nekoyume;
using Nekoyume.Extensions;
using Nekoyume.Helper;
using Nekoyume.Model.Item;
using Nekoyume.Model.Market;
using Nekoyume.Model.State;
using Nekoyume.TableData;
using Nekoyume.TableData.Summon;
using NineChronicles.RPC.Server.DataRendering;
using NineChronicles.RPC.Server.Store;
using NineChronicles.RPC.Server.Store.Models;
using static Lib9c.SerializeKeys;

namespace NineChronicles.RPC.Server.Executable.Hubs
{
    public class HeadlessHubReceiver :  IActionEvaluationHubReceiver, IHostedService, IDisposable
    {
        private readonly ChannelBase _grpcChannel;
        private readonly GrpcChannelOptions _grpcChannelOptions;
        private readonly ILogger<HeadlessHubReceiver> _logger;
        private readonly PrivateKey _privateKey = new PrivateKey();
        private IActionEvaluationHub? _headlessHub;
        private IActionEvaluationHub? _hub;
        private Codec _codec = new Codec();
        private const int DefaultInsertInterval = 1;
        private readonly int _blockInsertInterval;
        private readonly string _blockIndexFilePath;
        private readonly IBlockChainStates _blockChainStates;
        private readonly BlockRenderer _blockRenderer;
        private readonly ActionRenderer _actionRenderer;
        private readonly List<AgentModel> _agentList = new List<AgentModel>();
        private readonly List<AvatarModel> _avatarList = new List<AvatarModel>();
        private readonly List<HackAndSlashModel> _hasList = new List<HackAndSlashModel>();
        private readonly List<CombinationConsumableModel> _ccList = new List<CombinationConsumableModel>();
        private readonly List<CombinationEquipmentModel> _ceList = new List<CombinationEquipmentModel>();
        private readonly List<EquipmentModel> _eqList = new List<EquipmentModel>();
        private readonly List<ItemEnhancementModel> _ieList = new List<ItemEnhancementModel>();
        private readonly List<ShopHistoryEquipmentModel> _buyShopEquipmentsList = new List<ShopHistoryEquipmentModel>();
        private readonly List<ShopHistoryCostumeModel> _buyShopCostumesList = new List<ShopHistoryCostumeModel>();
        private readonly List<ShopHistoryMaterialModel> _buyShopMaterialsList = new List<ShopHistoryMaterialModel>();
        private readonly List<ShopHistoryConsumableModel> _buyShopConsumablesList = new List<ShopHistoryConsumableModel>();
        private readonly List<ShopHistoryFungibleAssetValueModel> _buyShopFavList = new List<ShopHistoryFungibleAssetValueModel>();
        private readonly List<StakeModel> _stakeList = new List<StakeModel>();
        private readonly List<ClaimStakeRewardModel> _claimStakeList = new List<ClaimStakeRewardModel>();
        private readonly List<MigrateMonsterCollectionModel> _mmcList = new List<MigrateMonsterCollectionModel>();
        private readonly List<GrindingModel> _grindList = new List<GrindingModel>();
        private readonly List<ItemEnhancementFailModel> _itemEnhancementFailList = new List<ItemEnhancementFailModel>();
        private readonly List<UnlockEquipmentRecipeModel> _unlockEquipmentRecipeList = new List<UnlockEquipmentRecipeModel>();
        private readonly List<UnlockWorldModel> _unlockWorldList = new List<UnlockWorldModel>();
        private readonly List<ReplaceCombinationEquipmentMaterialModel> _replaceCombinationEquipmentMaterialList = new List<ReplaceCombinationEquipmentMaterialModel>();
        private readonly List<HasRandomBuffModel> _hasRandomBuffList = new List<HasRandomBuffModel>();
        private readonly List<HasWithRandomBuffModel> _hasWithRandomBuffList = new List<HasWithRandomBuffModel>();
        private readonly List<JoinArenaModel> _joinArenaList = new List<JoinArenaModel>();
        private readonly List<BattleArenaModel> _battleArenaList = new List<BattleArenaModel>();
        private readonly List<BlockModel> _blockList = new List<BlockModel>();
        private readonly List<TransactionModel> _transactionList = new List<TransactionModel>();
        private readonly List<HackAndSlashSweepModel> _hasSweepList = new List<HackAndSlashSweepModel>();
        private readonly List<EventDungeonBattleModel> _eventDungeonBattleList = new List<EventDungeonBattleModel>();
        private readonly List<EventConsumableItemCraftsModel> _eventConsumableItemCraftsList = new List<EventConsumableItemCraftsModel>();
        private readonly List<RaiderModel> _raiderList = new List<RaiderModel>();
        private readonly List<BattleGrandFinaleModel> _battleGrandFinaleList = new List<BattleGrandFinaleModel>();
        private readonly List<EventMaterialItemCraftsModel> _eventMaterialItemCraftsList = new List<EventMaterialItemCraftsModel>();
        private readonly List<RuneEnhancementModel> _runeEnhancementList = new List<RuneEnhancementModel>();
        private readonly List<RunesAcquiredModel> _runesAcquiredList = new List<RunesAcquiredModel>();
        private readonly List<UnlockRuneSlotModel> _unlockRuneSlotList = new List<UnlockRuneSlotModel>();
        private readonly List<RapidCombinationModel> _rapidCombinationList = new List<RapidCombinationModel>();
        private readonly List<PetEnhancementModel> _petEnhancementList = new List<PetEnhancementModel>();
        private readonly List<TransferAssetModel> _transferAssetList = new List<TransferAssetModel>();
        private readonly List<RequestPledgeModel> _requestPledgeList = new List<RequestPledgeModel>();
        private readonly List<AuraSummonModel> _auraSummonList = new List<AuraSummonModel>();
        private readonly List<AuraSummonFailModel> _auraSummonFailList = new List<AuraSummonFailModel>();
        private readonly List<RuneSummonModel> _runeSummonList = new List<RuneSummonModel>();
        private readonly List<RuneSummonFailModel> _runeSummonFailList = new List<RuneSummonFailModel>();
        private readonly List<string> _agents;
        private readonly bool _render;
        private int _renderedBlockCount;
        private DateTimeOffset _blockTimeOffset;
        private Address _miner;
        private string? _blockHash;

        public HeadlessHubReceiver(
            ILogger<HeadlessHubReceiver> logger,
            IOptions<HeadlessConnectionOption> options,
            MySqlStore mySqlStore)
        {
            MySqlStore = mySqlStore;
            _renderedBlockCount = 0;
            _agents = new List<string>();
            _render = true;
            string dataPath = Environment.GetEnvironmentVariable("NC_BlockIndexFilePath")
                              ?? Path.GetTempPath();
            if (!Directory.Exists(dataPath))
            {
                dataPath = Path.GetTempPath();
            }

            _blockIndexFilePath = Path.Combine(dataPath, "blockIndex.txt");

            try
            {
                _blockInsertInterval = Convert.ToInt32(Environment.GetEnvironmentVariable("NC_BlockInsertInterval"));
                if (_blockInsertInterval < 1)
                {
                    _blockInsertInterval = DefaultInsertInterval;
                }
            }
            catch (Exception)
            {
                _blockInsertInterval = DefaultInsertInterval;
            }

            HeadlessConnectionOption option = options.Value;
            _grpcChannelOptions = new GrpcChannelOptions()
            {
                Credentials = ChannelCredentials.Insecure,
                MaxReceiveMessageSize = null,
            };
            _grpcChannel = GrpcChannel.ForAddress($"http://{option.Host}:{option.Port}", _grpcChannelOptions);
            _logger = logger;
        }

        public IBlockChainService GetHeadlessService()
        {
            if (BlockChainService is null)
            {
                throw new Exception("HeadlessService is null.");
            }
            return BlockChainService;
        }

        internal MySqlStore MySqlStore { get; }

        public IBlockChainService? BlockChainService
        {
            get;
            private set;
        }

        public void OnRender(byte[] evaluation)
        {
            try
            {
                if (_hub is null)
                {
                    var channel = GrpcChannel.ForAddress($"http://{IPAddress.Loopback.ToString()}:5250", _grpcChannelOptions); 
                    _hub = StreamingHubClient.ConnectAsync<IActionEvaluationHub, IActionEvaluationHubReceiver>(
                        channel,
                        null!
                    ).Result;
                    _hub.JoinAsync(_privateKey.Address.ToHex());
                    _logger.LogInformation("Connected to local ActionEvaluationHub.");
                }

            }
            catch (Exception)
            {
                // ignored
            }

            using (var cp = new MemoryStream(evaluation))
            using (var decompressed = new MemoryStream())
            using (var df = new DeflateStream(cp, CompressionMode.Decompress))
            {
                var resolver = MessagePack.Resolvers.CompositeResolver.Create(
                    NineChroniclesResolver.Instance,
                    StandardResolver.Instance
                );
                var options = MessagePackSerializerOptions.Standard.WithResolver(resolver);
                MessagePackSerializer.DefaultOptions = options;
                df.CopyTo(decompressed);
                decompressed.Seek(0, SeekOrigin.Begin);
                var dec = decompressed.ToArray();
                var ev = MessagePackSerializer.Deserialize<NCActionEvaluation>(dec, options).ToActionEvaluation();

                _logger.LogInformation(ev.Action.ToString());
                if (ev.Exception == null)
                {
                    if (ev.Action is ITransferAsset transferAsset)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var actionString = ev.TxId.ToString();
                        var actionByteArray = Encoding.UTF8.GetBytes(ev.OutputState.ToString()).Take(16).ToArray();
                        var id = new Guid(actionByteArray);
                        _transferAssetList.Add(TransferAssetData.GetTransferAssetInfo(
                            id,
                            (TxId)ev.TxId!,
                            ev.BlockIndex,
                            _blockHash!,
                            transferAsset.Sender,
                            transferAsset.Recipient,
                            transferAsset.Amount.Currency.Ticker,
                            transferAsset.Amount,
                            _blockTimeOffset));

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored TransferAsset action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is IClaimStakeReward claimStakeReward)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var plainValue = (Dictionary)claimStakeReward.PlainValue;
                        var avatarAddress = ((Dictionary)plainValue["values"])[AvatarAddressKey].ToAddress();
                        var id = ((GameAction)claimStakeReward).Id;
    #pragma warning disable CS0618
                        var runeCurrency = RuneHelper.StakeRune;
    #pragma warning restore CS0618
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        var prevRuneBalance = inputState.GetBalance(
                            avatarAddress,
                            runeCurrency);
                        var outputRuneBalance = outputState.GetBalance(
                            avatarAddress,
                            runeCurrency);
                        var acquiredRune = outputRuneBalance - prevRuneBalance;
                        var actionType = claimStakeReward.ToString()!.Split('.').LastOrDefault()?.Replace(">", string.Empty);
                        _runesAcquiredList.Add(RunesAcquiredData.GetRunesAcquiredInfo(
                            id,
                            ev.Signer,
                            avatarAddress,
                            ev.BlockIndex,
                            actionType!,
                            runeCurrency.Ticker,
                            acquiredRune,
                            _blockTimeOffset));
                        _claimStakeList.Add(ClaimStakeRewardData.GetClaimStakeRewardInfo(claimStakeReward, inputState, outputState, ev.Signer, ev.BlockIndex, _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored ClaimStakeReward action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is EventDungeonBattle eventDungeonBattle)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var actionType = eventDungeonBattle.ToString()!.Split('.').LastOrDefault()
                            ?.Replace(">", string.Empty);
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _eventDungeonBattleList.Add(EventDungeonBattleData.GetEventDungeonBattleInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            eventDungeonBattle.AvatarAddress,
                            eventDungeonBattle.EventScheduleId,
                            eventDungeonBattle.EventDungeonId,
                            eventDungeonBattle.EventDungeonStageId,
                            eventDungeonBattle.Foods.Count,
                            eventDungeonBattle.Costumes.Count,
                            eventDungeonBattle.Equipments.Count,
                            eventDungeonBattle.Id,
                            actionType!,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored EventDungeonBattle action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is EventConsumableItemCrafts eventConsumableItemCrafts)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _eventConsumableItemCraftsList.Add(EventConsumableItemCraftsData.GetEventConsumableItemCraftsInfo(eventConsumableItemCrafts, inputState, outputState, ev.Signer, ev.BlockIndex, _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored EventConsumableItemCrafts action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is RequestPledge requestPledge)
                    {
                        var start = DateTimeOffset.UtcNow;
                        _requestPledgeList.Add(RequestPledgeData.GetRequestPledgeInfo(ev.TxId.ToString()!, ev.BlockIndex, _blockHash!, ev.Signer, requestPledge.AgentAddress, requestPledge.RefillMead, _blockTimeOffset));

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored RequestPledge action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is HackAndSlash has)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _avatarList.Add(AvatarData.GetAvatarInfo(outputState, ev.Signer, has.AvatarAddress, has.RuneInfos, _blockTimeOffset));
                        _hasList.Add(HackAndSlashData.GetHackAndSlashInfo(inputState, outputState, ev.Signer, has.AvatarAddress, has.StageId, has.Id, ev.BlockIndex, _blockTimeOffset));
                        if (has.StageBuffId.HasValue)
                        {
                            _hasWithRandomBuffList.Add(HasWithRandomBuffData.GetHasWithRandomBuffInfo(inputState, outputState, ev.Signer, has.AvatarAddress, has.StageId, has.StageBuffId, has.Id, ev.BlockIndex, _blockTimeOffset));
                        }

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored HackAndSlash action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is HackAndSlashSweep hasSweep)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _avatarList.Add(AvatarData.GetAvatarInfo(outputState, ev.Signer, hasSweep.avatarAddress, hasSweep.runeInfos, _blockTimeOffset));
                        _hasSweepList.Add(HackAndSlashSweepData.GetHackAndSlashSweepInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            hasSweep.avatarAddress,
                            hasSweep.stageId,
                            hasSweep.worldId,
                            hasSweep.apStoneCount,
                            hasSweep.actionPoint,
                            hasSweep.costumes.Count,
                            hasSweep.equipments.Count,
                            hasSweep.Id,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored HackAndSlashSweep action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is CombinationConsumable combinationConsumable)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _ccList.Add(CombinationConsumableData.GetCombinationConsumableInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            combinationConsumable.avatarAddress,
                            combinationConsumable.recipeId,
                            combinationConsumable.slotIndex,
                            combinationConsumable.Id,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored CombinationConsumable action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is CombinationEquipment combinationEquipment)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _ceList.Add(CombinationEquipmentData.GetCombinationEquipmentInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            combinationEquipment.avatarAddress,
                            combinationEquipment.recipeId,
                            combinationEquipment.slotIndex,
                            combinationEquipment.subRecipeId,
                            combinationEquipment.Id,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        if (combinationEquipment.payByCrystal)
                        {
                            var replaceCombinationEquipmentMaterialList = ReplaceCombinationEquipmentMaterialData
                                .GetReplaceCombinationEquipmentMaterialInfo(
                                    inputState,
                                    outputState,
                                    ev.Signer,
                                    combinationEquipment.avatarAddress,
                                    combinationEquipment.recipeId,
                                    combinationEquipment.subRecipeId,
                                    combinationEquipment.payByCrystal,
                                    combinationEquipment.Id,
                                    ev.BlockIndex,
                                    _blockTimeOffset);
                            foreach (var replaceCombinationEquipmentMaterial in replaceCombinationEquipmentMaterialList)
                            {
                                _replaceCombinationEquipmentMaterialList.Add(replaceCombinationEquipmentMaterial);
                            }
                        }

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation(
                            "Stored CombinationEquipment action in block #{index}. Time Taken: {time} ms.",
                            ev.BlockIndex,
                            (end - start).Milliseconds);
                        start = DateTimeOffset.UtcNow;

                        var slotState = outputState.GetCombinationSlotState(
                            combinationEquipment.avatarAddress,
                            combinationEquipment.slotIndex);

                        if (slotState?.Result.itemUsable.ItemType is ItemType.Equipment)
                        {
                            _eqList.Add(EquipmentData.GetEquipmentInfo(
                                ev.Signer,
                                combinationEquipment.avatarAddress,
                                (Equipment)slotState.Result.itemUsable,
                                _blockTimeOffset));
                        }

                        end = DateTimeOffset.UtcNow;
                        _logger.LogInformation(
                            "Stored avatar {address}'s equipment in block #{index}. Time Taken: {time} ms.",
                            combinationEquipment.avatarAddress,
                            ev.BlockIndex,
                            (end - start).Milliseconds);
                    }

                    if (ev.Action is ItemEnhancement itemEnhancement)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        if (ItemEnhancementFailData.GetItemEnhancementFailInfo(
                                inputState,
                                outputState,
                                ev.Signer,
                                itemEnhancement.avatarAddress,
                                Guid.Empty,
                                itemEnhancement.materialIds,
                                itemEnhancement.itemId,
                                itemEnhancement.Id,
                                ev.BlockIndex,
                                _blockTimeOffset) is { } itemEnhancementFailModel)
                        {
                            _itemEnhancementFailList.Add(itemEnhancementFailModel);
                        }

                        _ieList.Add(ItemEnhancementData.GetItemEnhancementInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            itemEnhancement.avatarAddress,
                            itemEnhancement.slotIndex,
                            Guid.Empty,
                            itemEnhancement.materialIds,
                            itemEnhancement.itemId,
                            itemEnhancement.Id,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored ItemEnhancement action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                        start = DateTimeOffset.UtcNow;

                        var slotState = outputState.GetCombinationSlotState(
                            itemEnhancement.avatarAddress,
                            itemEnhancement.slotIndex);

                        if (slotState?.Result.itemUsable.ItemType is ItemType.Equipment)
                        {
                            _eqList.Add(EquipmentData.GetEquipmentInfo(
                                ev.Signer,
                                itemEnhancement.avatarAddress,
                                (Equipment)slotState.Result.itemUsable,
                                _blockTimeOffset));
                        }

                        end = DateTimeOffset.UtcNow;
                        _logger.LogInformation(
                            "Stored avatar {address}'s equipment in block #{index}. Time Taken: {time} ms.",
                            itemEnhancement.avatarAddress,
                            ev.BlockIndex,
                            (end - start).Milliseconds);
                    }

                    if (ev.Action is Buy buy)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        AvatarState avatarState = outputState.GetAvatarStateV2(buy.buyerAvatarAddress);
                        var buyerInventory = avatarState.inventory;
                        foreach (var purchaseInfo in buy.purchaseInfos)
                        {
                            var state = outputState.GetState(
                            Addresses.GetItemAddress(purchaseInfo.TradableId));
                            ITradableItem orderItem =
                                (ITradableItem)ItemFactory.Deserialize((Dictionary)state!);
                            Order order =
                                OrderFactory.Deserialize(
                                    (Dictionary)outputState.GetState(
                                        Order.DeriveAddress(purchaseInfo.OrderId))!);
                            int itemCount = order is FungibleOrder fungibleOrder
                                ? fungibleOrder.ItemCount
                                : 1;
                            AddShopHistoryItem(orderItem, buy.buyerAvatarAddress, purchaseInfo, itemCount, ev.BlockIndex);

                            if (purchaseInfo.ItemSubType == ItemSubType.Armor
                                || purchaseInfo.ItemSubType == ItemSubType.Belt
                                || purchaseInfo.ItemSubType == ItemSubType.Necklace
                                || purchaseInfo.ItemSubType == ItemSubType.Ring
                                || purchaseInfo.ItemSubType == ItemSubType.Weapon)
                            {
                                var sellerState = outputState.GetAvatarStateV2(purchaseInfo.SellerAvatarAddress);
                                var sellerInventory = sellerState.inventory;

                                if (buyerInventory.Equipments == null || sellerInventory.Equipments == null)
                                {
                                    continue;
                                }

                                Equipment? equipment = buyerInventory.Equipments.SingleOrDefault(i =>
                                    i.ItemId == purchaseInfo.TradableId) ?? sellerInventory.Equipments.SingleOrDefault(i =>
                                    i.ItemId == purchaseInfo.TradableId);

                                if (equipment is { } equipmentNotNull)
                                {
                                    _eqList.Add(EquipmentData.GetEquipmentInfo(
                                        ev.Signer,
                                        buy.buyerAvatarAddress,
                                        equipmentNotNull,
                                        _blockTimeOffset));
                                }
                            }
                        }

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation(
                            "Stored avatar {address}'s equipment in block #{index}. Time Taken: {time} ms.",
                            buy.buyerAvatarAddress,
                            ev.BlockIndex,
                            (end - start).Milliseconds);
                    }

                    if (ev.Action is BuyProduct buyProduct)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        foreach (var productInfo in buyProduct.ProductInfos)
                        {
                            switch (productInfo)
                            {
                                case FavProductInfo _:
                                    // Check previous product state. because Set Bencodex.Types.Null in BuyProduct.
                                    if (inputState.TryGetState(Product.DeriveAddress(productInfo.ProductId), out List productState))
                                    {
                                        var favProduct = (FavProduct)ProductFactory.DeserializeProduct(productState);
                                        _buyShopFavList.Add(new ShopHistoryFungibleAssetValueModel
                                        {
                                            OrderId = productInfo.ProductId.ToString(),
                                            TxId = ev.TxId.ToString(),
                                            BlockIndex = ev.BlockIndex,
                                            BlockHash = _blockHash,
                                            SellerAvatarAddress = productInfo.AvatarAddress.ToString(),
                                            BuyerAvatarAddress = buyProduct.AvatarAddress.ToString(),
                                            Price = decimal.Parse(productInfo.Price.GetQuantityString()),
                                            Quantity = decimal.Parse(favProduct.Asset.GetQuantityString()),
                                            Ticker = favProduct.Asset.Currency.Ticker,
                                            TimeStamp = _blockTimeOffset,
                                        });
                                    }

                                    break;
                                case ItemProductInfo itemProductInfo:
                                {
                                    ITradableItem orderItem;
                                    int itemCount = 1;

                                    // backward compatibility for order.
                                    if (itemProductInfo.Legacy)
                                    {
                                        var state = outputState.GetState(
                                            Addresses.GetItemAddress(itemProductInfo.TradableId));
                                        orderItem =
                                            (ITradableItem)ItemFactory.Deserialize((Dictionary)state!);
                                        Order order =
                                            OrderFactory.Deserialize(
                                                (Dictionary)outputState.GetState(
                                                    Order.DeriveAddress(itemProductInfo.ProductId))!);
                                        itemCount = order is FungibleOrder fungibleOrder
                                            ? fungibleOrder.ItemCount
                                            : 1;
                                    }
                                    else
                                    {
                                        // Check previous product state. because Set Bencodex.Types.Null in BuyProduct.
                                        if (inputState.TryGetState(Product.DeriveAddress(productInfo.ProductId), out List state))
                                        {
                                            var product = (ItemProduct)ProductFactory.DeserializeProduct(state);
                                            orderItem = product.TradableItem;
                                            itemCount = product.ItemCount;
                                        }
                                        else
                                        {
                                            continue;
                                        }
                                    }

                                    var purchaseInfo = new PurchaseInfo(
                                        productInfo.ProductId,
                                        itemProductInfo.TradableId,
                                        productInfo.AgentAddress,
                                        productInfo.AvatarAddress,
                                        itemProductInfo.ItemSubType,
                                        productInfo.Price
                                    );
                                    AddShopHistoryItem(orderItem, buyProduct.AvatarAddress, purchaseInfo, itemCount, ev.BlockIndex);
                                    if (orderItem.ItemType == ItemType.Equipment)
                                    {
                                        var equipment = (Equipment)orderItem;
                                        _eqList.Add(EquipmentData.GetEquipmentInfo(
                                            ev.Signer,
                                            buyProduct.AvatarAddress,
                                            equipment,
                                            _blockTimeOffset));
                                    }

                                    break;
                                }
                            }
                        }

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation(
                            "Stored avatar {address}'s equipment in block #{index}. Time Taken: {time} ms.",
                            buyProduct.AvatarAddress,
                            ev.BlockIndex,
                            (end - start).Milliseconds);
                    }

                    if (ev.Action is Stake stake)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _stakeList.Add(StakeData.GetStakeInfo(inputState, outputState, ev.Signer, ev.BlockIndex, _blockTimeOffset, stake.Id));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored Stake action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is MigrateMonsterCollection mc)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _mmcList.Add(MigrateMonsterCollectionData.GetMigrateMonsterCollectionInfo(inputState, outputState, ev.Signer, ev.BlockIndex, _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored MigrateMonsterCollection action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is Grinding grinding)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));

                        var grindList = GrindingData.GetGrindingInfo(inputState, outputState, ev.Signer, grinding.AvatarAddress, grinding.EquipmentIds, grinding.Id, ev.BlockIndex, _blockTimeOffset);

                        foreach (var grind in grindList)
                        {
                            _grindList.Add(grind);
                        }

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored Grinding action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is UnlockEquipmentRecipe unlockEquipmentRecipe)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        var unlockEquipmentRecipeList = UnlockEquipmentRecipeData.GetUnlockEquipmentRecipeInfo(inputState, outputState, ev.Signer, unlockEquipmentRecipe.AvatarAddress, unlockEquipmentRecipe.RecipeIds, unlockEquipmentRecipe.Id, ev.BlockIndex, _blockTimeOffset);
                        foreach (var unlockEquipmentRecipeData in unlockEquipmentRecipeList)
                        {
                            _unlockEquipmentRecipeList.Add(unlockEquipmentRecipeData);
                        }

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored UnlockEquipmentRecipe action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is UnlockWorld unlockWorld)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        var unlockWorldList = UnlockWorldData.GetUnlockWorldInfo(inputState, outputState, ev.Signer, unlockWorld.AvatarAddress, unlockWorld.WorldIds, unlockWorld.Id, ev.BlockIndex, _blockTimeOffset);
                        foreach (var unlockWorldData in unlockWorldList)
                        {
                            _unlockWorldList.Add(unlockWorldData);
                        }

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored UnlockWorld action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is HackAndSlashRandomBuff hasRandomBuff)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _hasRandomBuffList.Add(HackAndSlashRandomBuffData.GetHasRandomBuffInfo(inputState, outputState, ev.Signer, hasRandomBuff.AvatarAddress, hasRandomBuff.AdvancedGacha, hasRandomBuff.Id, ev.BlockIndex, _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored HasRandomBuff action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is JoinArena joinArena)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _joinArenaList.Add(JoinArenaData.GetJoinArenaInfo(inputState, outputState, ev.Signer, joinArena.avatarAddress, joinArena.round, joinArena.championshipId, joinArena.Id, ev.BlockIndex, _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored JoinArena action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is BattleArena battleArena)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _avatarList.Add(AvatarData.GetAvatarInfo(outputState, ev.Signer, battleArena.myAvatarAddress, battleArena.runeInfos, _blockTimeOffset));
                        _battleArenaList.Add(BattleArenaData.GetBattleArenaInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            battleArena.myAvatarAddress,
                            battleArena.enemyAvatarAddress,
                            battleArena.round,
                            battleArena.championshipId,
                            battleArena.ticket,
                            battleArena.Id,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored BattleArena action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is BattleGrandFinale battleGrandFinale)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _battleGrandFinaleList.Add(BattleGrandFinaleData.GetBattleGrandFinaleInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            battleGrandFinale.myAvatarAddress,
                            battleGrandFinale.enemyAvatarAddress,
                            battleGrandFinale.grandFinaleId,
                            battleGrandFinale.Id,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored BattleGrandFinale action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is EventMaterialItemCrafts eventMaterialItemCrafts)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _eventMaterialItemCraftsList.Add(EventMaterialItemCraftsData.GetEventMaterialItemCraftsInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            eventMaterialItemCrafts.AvatarAddress,
                            eventMaterialItemCrafts.MaterialsToUse,
                            eventMaterialItemCrafts.EventScheduleId,
                            eventMaterialItemCrafts.EventMaterialItemRecipeId,
                            eventMaterialItemCrafts.Id,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored EventMaterialItemCrafts action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is RuneEnhancement runeEnhancement)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _runeEnhancementList.Add(RuneEnhancementData.GetRuneEnhancementInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            runeEnhancement.AvatarAddress,
                            runeEnhancement.RuneId,
                            runeEnhancement.TryCount,
                            runeEnhancement.Id,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored RuneEnhancement action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is TransferAssets transferAssets)
                    {
                        var start = DateTimeOffset.UtcNow;
                        int count = 0;
                        foreach (var recipient in transferAssets.Recipients)
                        {
                            var actionString = count + ev.TxId.ToString();
                            var actionByteArray = Encoding.UTF8.GetBytes(actionString).Take(16).ToArray();
                            var id = new Guid(actionByteArray);
                            var avatarAddress = recipient.recipient;
                            var actionType = transferAssets.ToString()!.Split('.').LastOrDefault()
                                ?.Replace(">", string.Empty);
                            _transferAssetList.Add(TransferAssetData.GetTransferAssetInfo(
                                id,
                                (TxId)ev.TxId!,
                                ev.BlockIndex,
                                _blockHash!,
                                transferAssets.Sender,
                                recipient.recipient,
                                recipient.amount.Currency.Ticker,
                                recipient.amount,
                                _blockTimeOffset));
                            _runesAcquiredList.Add(RunesAcquiredData.GetRunesAcquiredInfo(
                                id,
                                ev.Signer,
                                avatarAddress,
                                ev.BlockIndex,
                                actionType!,
                                recipient.amount.Currency.Ticker,
                                recipient.amount,
                                _blockTimeOffset));
                            count++;
                        }

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored TransferAssets action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is DailyReward dailyReward)
                    {
                        var start = DateTimeOffset.UtcNow;
#pragma warning disable CS0618
                        var runeCurrency = RuneHelper.DailyRewardRune;
#pragma warning restore CS0618
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        var prevRuneBalance = inputState.GetBalance(
                            dailyReward.avatarAddress,
                            runeCurrency);
                        var outputRuneBalance = outputState.GetBalance(
                            dailyReward.avatarAddress,
                            runeCurrency);
                        var acquiredRune = outputRuneBalance - prevRuneBalance;
                        var actionType = dailyReward.ToString()!.Split('.').LastOrDefault()
                            ?.Replace(">", string.Empty);
                        _runesAcquiredList.Add(RunesAcquiredData.GetRunesAcquiredInfo(
                            dailyReward.Id,
                            ev.Signer,
                            dailyReward.avatarAddress,
                            ev.BlockIndex,
                            actionType!,
                            runeCurrency.Ticker,
                            acquiredRune,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored DailyReward action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is ClaimRaidReward claimRaidReward)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        var sheets = outputState.GetSheets(
                            sheetTypes: new[]
                            {
                                typeof(RuneSheet),
                            });
                        var runeSheet = sheets.GetSheet<RuneSheet>();
                        foreach (var runeType in runeSheet.Values)
                        {
#pragma warning disable CS0618
                            var runeCurrency = RuneHelper.ToCurrency(runeType);
#pragma warning restore CS0618
                            var prevRuneBalance = inputState.GetBalance(
                                claimRaidReward.AvatarAddress,
                                runeCurrency);
                            var outputRuneBalance = outputState.GetBalance(
                                claimRaidReward.AvatarAddress,
                                runeCurrency);
                            var acquiredRune = outputRuneBalance - prevRuneBalance;
                            var actionType = claimRaidReward.ToString()!.Split('.').LastOrDefault()
                                ?.Replace(">", string.Empty);
                            if (Convert.ToDecimal(acquiredRune.GetQuantityString()) > 0)
                            {
                                _runesAcquiredList.Add(RunesAcquiredData.GetRunesAcquiredInfo(
                                    claimRaidReward.Id,
                                    ev.Signer,
                                    claimRaidReward.AvatarAddress,
                                    ev.BlockIndex,
                                    actionType!,
                                    runeCurrency.Ticker,
                                    acquiredRune,
                                    _blockTimeOffset));
                            }
                        }

                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored ClaimRaidReward action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is UnlockRuneSlot unlockRuneSlot)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _unlockRuneSlotList.Add(UnlockRuneSlotData.GetUnlockRuneSlotInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            unlockRuneSlot.AvatarAddress,
                            unlockRuneSlot.SlotIndex,
                            unlockRuneSlot.Id,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored UnlockRuneSlot action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }
                    if (ev.Action is RapidCombination rapidCombination)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _rapidCombinationList.Add(RapidCombinationData.GetRapidCombinationInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            rapidCombination.avatarAddress,
                            rapidCombination.slotIndex,
                            rapidCombination.Id,
                            ev.BlockIndex,
                            _blockTimeOffset));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored RapidCombination action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                    }

                    if (ev.Action is Raid raid)
                    {
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        var sheets = outputState.GetSheets(
                            sheetTypes: new[]
                            {
                                typeof(CharacterSheet),
                                typeof(CostumeStatSheet),
                                typeof(RuneSheet),
                                typeof(RuneListSheet),
                                typeof(RuneOptionSheet),
                                typeof(WorldBossListSheet),
                            });

                        var runeSheet = sheets.GetSheet<RuneSheet>();
                        foreach (var runeType in runeSheet.Values)
                        {
#pragma warning disable CS0618
                            var runeCurrency = RuneHelper.ToCurrency(runeType);
#pragma warning restore CS0618
                            var prevRuneBalance = inputState.GetBalance(
                                raid.AvatarAddress,
                                runeCurrency);
                            var outputRuneBalance = outputState.GetBalance(
                                raid.AvatarAddress,
                                runeCurrency);
                            var acquiredRune = outputRuneBalance - prevRuneBalance;
                            var actionType = raid.ToString()!.Split('.').LastOrDefault()
                                ?.Replace(">", string.Empty);
                            if (Convert.ToDecimal(acquiredRune.GetQuantityString()) > 0)
                            {
                                _runesAcquiredList.Add(RunesAcquiredData.GetRunesAcquiredInfo(
                                    raid.Id,
                                    ev.Signer,
                                    raid.AvatarAddress,
                                    ev.BlockIndex,
                                    actionType!,
                                    runeCurrency.Ticker,
                                    acquiredRune,
                                    _blockTimeOffset));
                            }
                        }

                        _avatarList.Add(AvatarData.GetAvatarInfo(outputState, ev.Signer, raid.AvatarAddress, raid.RuneInfos, _blockTimeOffset));

                        var worldBossListSheet = sheets.GetSheet<WorldBossListSheet>();
                        int raidId = worldBossListSheet.FindRaidIdByBlockIndex(ev.BlockIndex);
                        RaiderState raiderState =
                            outputState.GetRaiderState(raid.AvatarAddress, raidId);
                        var model = new RaiderModel(
                            raidId,
                            raiderState.AvatarName,
                            raiderState.HighScore,
                            raiderState.TotalScore,
                            raiderState.Cp,
                            raiderState.IconId,
                            raiderState.Level,
                            raiderState.AvatarAddress.ToHex(),
                            raiderState.PurchaseCount);
                        _raiderList.Add(RaidData.GetRaidInfo(raidId, raiderState));
                        MySqlStore.StoreRaider(model);
                    }

                    if (ev.Action is PetEnhancement petEnhancement)
                    {
                        var start = DateTimeOffset.UtcNow;
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        _petEnhancementList.Add(PetEnhancementData.GetPetEnhancementInfo(
                            inputState,
                            outputState,
                            ev.Signer,
                            petEnhancement.AvatarAddress,
                            petEnhancement.PetId,
                            petEnhancement.TargetLevel,
                            petEnhancement.Id,
                            ev.BlockIndex,
                            _blockTimeOffset
                        ));
                        var end = DateTimeOffset.UtcNow;
                        _logger.LogInformation("Stored PetEnhancement action in block #{BlockIndex}. Time taken: {Time} ms", ev.BlockIndex, end - start);
                    }
                    if (ev.Action is AuraSummon auraSummon)
                    {
                        var inputState = new Account(_blockChainStates.GetAccountState(ev.PreviousState));
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        if (ev.Exception is null)
                        {
                            _auraSummonList.Add(AuraSummonData.GetAuraSummonInfo(
                                inputState,
                                outputState,
                                ev.Signer,
                                auraSummon.AvatarAddress,
                                auraSummon.GroupId,
                                auraSummon.SummonCount,
                                auraSummon.Id,
                                ev.BlockIndex
                                ));
                        }
                        else
                        {
                            _auraSummonFailList.Add(AuraSummonData.GetAuraSummonFailInfo(
                                inputState,
                                outputState,
                                ev.Signer,
                                auraSummon.AvatarAddress,
                                auraSummon.GroupId,
                                auraSummon.SummonCount,
                                auraSummon.Id,
                                ev.BlockIndex,
                                ev.Exception
                            ));
                        }
                    }
                    if (ev.Action is RuneSummon runeSummon)
                    {
                        var outputState = new Account(_blockChainStates.GetAccountState(ev.OutputState));
                        if (ev.Exception is null)
                        {
                            var sheets = outputState.GetSheets(
                                sheetTypes: new[]
                                {
                                    typeof(RuneSheet),
                                    typeof(SummonSheet),
                                });
                            var runeSheet = sheets.GetSheet<RuneSheet>();
                            var summonSheet = sheets.GetSheet<SummonSheet>();
                            _runeSummonList.Add(RuneSummonData.GetRuneSummonInfo(
                                ev.Signer,
                                runeSummon.AvatarAddress,
                                runeSummon.GroupId,
                                runeSummon.SummonCount,
                                runeSummon.Id,
                                ev.BlockIndex,
                                runeSheet,
                                summonSheet,
                                new ReplayRandom(ev.RandomSeed)
                            ));
                        }
                        else
                        {
                            _runeSummonFailList.Add(RuneSummonData.GetRuneSummonFailInfo(
                                ev.Signer,
                                runeSummon.AvatarAddress,
                                runeSummon.GroupId,
                                runeSummon.SummonCount,
                                runeSummon.Id,
                                ev.BlockIndex,
                                ev.Exception
                            ));
                        }
                    }
                }
            }

            _hub?.BroadcastRenderAsync(evaluation);
        }

        public void OnUnrender(byte[] evaluation)
        {
            _hub?.BroadcastUnrenderAsync(evaluation);
        }

        public void OnRenderBlock(byte[] oldTip, byte[] newTip)
        {
            Stopwatch stopWatch = new Stopwatch();
            _logger.LogInformation("Start {Method}", nameof(OnRenderBlock));
            stopWatch.Start();
            var codec = new Codec();
            var oldBlock = BlockMarshaler.UnmarshalBlock((Dictionary)codec.Decode(oldTip));
            var newBlock = BlockMarshaler.UnmarshalBlock((Dictionary)codec.Decode(newTip));
            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;
            _logger.LogInformation(
                "Block render from {OldTipIndex} to {NewTipIndex} at {TimeTaken}",
                oldBlock.Index,
                newBlock.Index,
                ts);
            if (_renderedBlockCount == _blockInsertInterval)
            {
                StoreRenderedData(oldBlock, newBlock);
            }

            var block = newBlock;
            _blockTimeOffset = block.Timestamp.UtcDateTime;
            _blockHash = block.Hash.ToString();
            _miner = block.Miner;
            _blockList.Add(BlockData.GetBlockInfo(block));

            foreach (var transaction in block.Transactions)
            {
                _transactionList.Add(TransactionData.GetTransactionInfo(block, transaction));
            }

            _renderedBlockCount++;
            _logger.LogInformation($"Rendered Block Count: #{_renderedBlockCount} at Block #{block.Index}");
            _hub?.BroadcastRenderBlockAsync(oldTip, newTip);
        }

        public void OnReorged(byte[] oldTip, byte[] newTip, byte[] branchpoint)
        {
            _hub?.ReportReorgAsync(oldTip, newTip, branchpoint);
        }

        public void OnReorgEnd(byte[] oldTip, byte[] newTip, byte[] branchpoint)
        {
            _hub?.ReportReorgEndAsync(oldTip, newTip, branchpoint);
        }

        public void OnException(int code, string message)
        {
            _hub?.ReportExceptionAsync(code, message);
        }

        public void OnPreloadStart()
        {
            _hub?.PreloadStartAsync();
        }

        public void OnPreloadEnd()
        {
            _hub?.PreloadEndAsync();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _headlessHub = await StreamingHubClient.ConnectAsync<IActionEvaluationHub, IActionEvaluationHubReceiver>(
                _grpcChannel, 
                this,
                cancellationToken: cancellationToken);
            BlockChainService = MagicOnionClient.Create<IBlockChainService>(_grpcChannel);
            await _headlessHub.JoinAsync(_privateKey.Address.ToHex());
            await BlockChainService.AddClient(_privateKey.Address.ToByteArray());
            await BlockChainService.SetAddressesToSubscribe(_privateKey.Address.ToByteArray(), new []
            {
                _privateKey.Address.ToByteArray(),
            });
            _logger.LogInformation("Connected to ActionEvaluationHub.");
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _headlessHub?.LeaveAsync();
            _hub?.LeaveAsync();
            _logger.LogInformation("Disconnected from ActionEvaluationHub.");
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _headlessHub?.DisposeAsync();
            _hub?.DisposeAsync();
        }

        private void AddShopHistoryItem(ITradableItem orderItem, Address buyerAvatarAddress, PurchaseInfo purchaseInfo, int itemCount, long blockIndex)
        {
            if (orderItem.ItemType == ItemType.Equipment)
            {
                Equipment equipment = (Equipment)orderItem;
                _buyShopEquipmentsList.Add(ShopHistoryEquipmentData.GetShopHistoryEquipmentInfo(
                    buyerAvatarAddress,
                    purchaseInfo,
                    equipment,
                    itemCount,
                    blockIndex,
                    _blockTimeOffset));
            }

            if (orderItem.ItemType == ItemType.Costume)
            {
                Costume costume = (Costume)orderItem;
                _buyShopCostumesList.Add(ShopHistoryCostumeData.GetShopHistoryCostumeInfo(
                    buyerAvatarAddress,
                    purchaseInfo,
                    costume,
                    itemCount,
                    blockIndex,
                    _blockTimeOffset));
            }

            if (orderItem.ItemType == ItemType.Material)
            {
                Material material = (Material)orderItem;
                _buyShopMaterialsList.Add(ShopHistoryMaterialData.GetShopHistoryMaterialInfo(
                    buyerAvatarAddress,
                    purchaseInfo,
                    material,
                    itemCount,
                    blockIndex,
                    _blockTimeOffset));
            }

            if (orderItem.ItemType == ItemType.Consumable)
            {
                Consumable consumable = (Consumable)orderItem;
                _buyShopConsumablesList.Add(ShopHistoryConsumableData.GetShopHistoryConsumableInfo(
                    buyerAvatarAddress,
                    purchaseInfo,
                    consumable,
                    itemCount,
                    blockIndex,
                    _blockTimeOffset));
            }
        }

        private void StoreRenderedData(Block OldTip, Block NewTip)
        {
            var start = DateTimeOffset.Now;
            _logger.LogInformation("Storing Data...");
            var tasks = new List<Task>
            {
                Task.Run(() =>
                {
                    MySqlStore.StoreAgentList(_agentList.GroupBy(i => i.Address).Select(i => i.FirstOrDefault())
                        .ToList());
                    MySqlStore.StoreAvatarList(_avatarList.GroupBy(i => i.Address).Select(i => i.FirstOrDefault())
                        .ToList());
                    MySqlStore.StoreHackAndSlashList(_hasList.GroupBy(i => i.Id).Select(i => i.FirstOrDefault())
                        .ToList());
                    MySqlStore.StoreCombinationConsumableList(_ccList.GroupBy(i => i.Id).Select(i => i.FirstOrDefault())
                        .ToList());
                    MySqlStore.StoreCombinationEquipmentList(_ceList.GroupBy(i => i.Id).Select(i => i.FirstOrDefault())
                        .ToList());
                    MySqlStore.StoreItemEnhancementList(_ieList.GroupBy(i => i.Id).Select(i => i.FirstOrDefault())
                        .ToList());
                    MySqlStore.StoreShopHistoryEquipmentList(_buyShopEquipmentsList.GroupBy(i => i.OrderId)
                        .Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreShopHistoryCostumeList(_buyShopCostumesList.GroupBy(i => i.OrderId)
                        .Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreShopHistoryMaterialList(_buyShopMaterialsList.GroupBy(i => i.OrderId)
                        .Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreShopHistoryConsumableList(_buyShopConsumablesList.GroupBy(i => i.OrderId)
                        .Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreShopHistoryFungibleAssetValues(_buyShopFavList);
                    MySqlStore.ProcessEquipmentList(_eqList.GroupBy(i => i.ItemId).Select(i => i.FirstOrDefault())
                        .ToList());
                    MySqlStore.StoreStakingList(_stakeList);
                    MySqlStore.StoreClaimStakeRewardList(_claimStakeList);
                    MySqlStore.StoreMigrateMonsterCollectionList(_mmcList);
                    MySqlStore.StoreGrindList(_grindList);
                    MySqlStore.StoreItemEnhancementFailList(_itemEnhancementFailList);
                    MySqlStore.StoreUnlockEquipmentRecipeList(_unlockEquipmentRecipeList);
                    MySqlStore.StoreUnlockWorldList(_unlockWorldList);
                    MySqlStore.StoreReplaceCombinationEquipmentMaterialList(_replaceCombinationEquipmentMaterialList);
                    MySqlStore.StoreHasRandomBuffList(_hasRandomBuffList);
                    MySqlStore.StoreHasWithRandomBuffList(_hasWithRandomBuffList);
                    MySqlStore.StoreJoinArenaList(_joinArenaList);
                    MySqlStore.StoreBattleArenaList(_battleArenaList);
                    MySqlStore.StoreBlockList(_blockList);
                    MySqlStore.StoreTransactionList(_transactionList);
                    MySqlStore.StoreHackAndSlashSweepList(_hasSweepList);
                    MySqlStore.StoreEventDungeonBattleList(_eventDungeonBattleList);
                    MySqlStore.StoreEventConsumableItemCraftsList(_eventConsumableItemCraftsList);
                    MySqlStore.StoreRaiderList(_raiderList);
                    MySqlStore.StoreBattleGrandFinaleList(_battleGrandFinaleList);
                    MySqlStore.StoreEventMaterialItemCraftsList(_eventMaterialItemCraftsList);
                    MySqlStore.StoreRuneEnhancementList(_runeEnhancementList);
                    MySqlStore.StoreRunesAcquiredList(_runesAcquiredList);
                    MySqlStore.StoreUnlockRuneSlotList(_unlockRuneSlotList);
                    MySqlStore.StoreRapidCombinationList(_rapidCombinationList);
                    MySqlStore.StorePetEnhancementList(_petEnhancementList);
                    MySqlStore.StoreTransferAssetList(_transferAssetList);
                    MySqlStore.StoreRequestPledgeList(_requestPledgeList);
                    MySqlStore.StoreAuraSummonList(_auraSummonList);
                    MySqlStore.StoreAuraSummonFailList(_auraSummonFailList);
                    MySqlStore.StoreRuneSummonList(_runeSummonList);
                    MySqlStore.StoreRuneSummonFailList(_runeSummonFailList);
                }),
            };

            Task.WaitAll(tasks.ToArray());
            _renderedBlockCount = 0;
            _agents.Clear();
            _agentList.Clear();
            _avatarList.Clear();
            _hasList.Clear();
            _ccList.Clear();
            _ceList.Clear();
            _ieList.Clear();
            _buyShopEquipmentsList.Clear();
            _buyShopCostumesList.Clear();
            _buyShopMaterialsList.Clear();
            _buyShopConsumablesList.Clear();
            _buyShopFavList.Clear();
            _eqList.Clear();
            _stakeList.Clear();
            _claimStakeList.Clear();
            _mmcList.Clear();
            _grindList.Clear();
            _itemEnhancementFailList.Clear();
            _unlockEquipmentRecipeList.Clear();
            _unlockWorldList.Clear();
            _replaceCombinationEquipmentMaterialList.Clear();
            _hasRandomBuffList.Clear();
            _hasWithRandomBuffList.Clear();
            _joinArenaList.Clear();
            _battleArenaList.Clear();
            _blockList.Clear();
            _transactionList.Clear();
            _hasSweepList.Clear();
            _eventDungeonBattleList.Clear();
            _eventConsumableItemCraftsList.Clear();
            _raiderList.Clear();
            _battleGrandFinaleList.Clear();
            _eventMaterialItemCraftsList.Clear();
            _runeEnhancementList.Clear();
            _runesAcquiredList.Clear();
            _unlockRuneSlotList.Clear();
            _rapidCombinationList.Clear();
            _petEnhancementList.Clear();
            _transferAssetList.Clear();
            _requestPledgeList.Clear();
            _auraSummonList.Clear();
            _auraSummonFailList.Clear();

            var end = DateTimeOffset.Now;
            long blockIndex = OldTip.Index;
            StreamWriter blockIndexFile = new StreamWriter(_blockIndexFilePath);
            blockIndexFile.Write(blockIndex);
            blockIndexFile.Flush();
            blockIndexFile.Close();
            _logger.LogInformation($"Storing Data Complete. Time Taken: {(end - start).Milliseconds} ms.");
        }
    }
}