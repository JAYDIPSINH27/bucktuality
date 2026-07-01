namespace Bucktuality.Shared.Contracts;

public class MatchRequest
{ 
    public string UserId { get; set; } = string.Empty;
    public string ConnectionId { get; set; }= string.Empty;

    public string? Vibe { get; set; }
}