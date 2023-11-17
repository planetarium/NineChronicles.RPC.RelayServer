namespace NineChronicles.RPC.Server.Executable
{
    public interface IStateCache
    {
        public bool TryGetState(byte[] addressBytes, byte[] blockHashBytes, out byte[] stateBytes);
        
        public bool TrySetState(byte[] addressBytes, byte[] blockHashBytes, byte[] stateBytes);
        
        public bool TryGetStateBySrh(byte[] addressBytes, byte[] stateRootHashBytes, out byte[] stateBytes);
        
        public bool TrySetStateBySrh(byte[] addressBytes, byte[] stateRootHashBytes, byte[] stateBytes);
        
        public bool TryGetBalance(byte[] addressBytes, byte[] currencyBytes, byte[] blockHashBytes, out byte[] stateBytes);
        
        public bool TrySetBalance(byte[] addressBytes, byte[] currencyBytes, byte[] blockHashBytes, byte[] balanceBytes);

        public bool TryGetBalanceBySrh(byte[] addressBytes, byte[] currencyBytes, byte[] stateRootHashBytes, out byte[] stateBytes);
        
        public bool TrySetBalanceBySrh(byte[] addressBytes, byte[] currencyBytes, byte[] stateRootHashBytes, byte[] balanceBytes);
    }
}