namespace Bucktuality.UserService.Models;

public class User
{
    public Guid Id { get; set; }
    public string DisplayName { get; set; } = string.Empty;
    public string? Vibe { get; set; }
    public bool IsAnonymous { get; set; } = true;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
}