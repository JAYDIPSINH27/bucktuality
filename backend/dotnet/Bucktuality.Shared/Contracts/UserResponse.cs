namespace Bucktuality.Shared.Contracts;

public class UserResponse
{
    public Guid UserId { get; set; }
    public string DisplayName { get; set; } = string.Empty;
    public string? Vibe { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}