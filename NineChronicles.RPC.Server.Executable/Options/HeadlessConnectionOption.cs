using Microsoft.Extensions.Options;

namespace NineChronicles.RPC.Server.Executable.Options
{
    public class HeadlessConnectionOption
    {
        public string Host { get; set; } = "localhost";
        public int Port { get; set; } = 65535;
    }
}