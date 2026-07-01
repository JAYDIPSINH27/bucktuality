using System.Net.Http.Json;
using Bucktuality.Shared.Contracts;

namespace Bucktuality.RealtimeService.Services;

public class MatchmakingClient
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<MatchmakingClient> _logger;

    public MatchmakingClient(HttpClient httpClient, ILogger<MatchmakingClient> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    public async Task<MatchResponse?> StartMatchAsync(MatchRequest request)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync("/match/start", request);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Matchmaking service returned {StatusCode}", response.StatusCode);
                return null;
            }

            return await response.Content.ReadFromJsonAsync<MatchResponse>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to call matchmaking service");
            return null;
        }
    }

    public async Task LeaveMatchAsync(string connectionId, string? roomId)
    {
        try
        {
            await _httpClient.PostAsJsonAsync("/match/leave", new
            {
                connectionId,
                roomId
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to leave matchmaking");
        }
    }
}