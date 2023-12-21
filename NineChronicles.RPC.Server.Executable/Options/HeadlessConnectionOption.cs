using Microsoft.Extensions.Options;

namespace NineChronicles.RPC.Server.Executable.Options
{
    public class HeadlessConnectionOption
    {
        public string Host { get; set; } = "9c-main-test-1.nine-chronicles.com";

        public int Port { get; set; } = 31238;

        public string MySqlConnectionString { get; set; } =
            "server=localhost;database=data_provider_ef14;uid=root;pwd=admin;port=3306";
    }
}