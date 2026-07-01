using System.Net.Http.Json;
using Bucktuality.Shared.Contracts;

namespace Bucktuality.RealtimeService.Services;

public class SessionClient
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<SessionClient> _logger;

    public SessionClient(HttpClient httpClient, ILogger<SessionClient> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    public async Task CreateSessionAsync(CreateSessionRequest request)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync("/sessions", request);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Session service failed with {StatusCode}", response.StatusCode);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create session");
        }
    }

    public async Task EndSessionAsync(string roomId)
    {
        try
        {
            await _httpClient.PostAsJsonAsync("/sessions/end", new EndSessionRequest
            {
                RoomId = roomId
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to end session");
        }
    }
}