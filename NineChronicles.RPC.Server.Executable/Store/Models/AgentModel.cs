namespace NineChronicles.RPC.Server.Store.Models
{
    using System.ComponentModel.DataAnnotations;

    public class AgentModel
    {
        [Key]
        public string? Address { get; set; }
    }
}
