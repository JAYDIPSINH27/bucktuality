using System.Net.Http.Json;
using Bucktuality.Shared.Contracts;

namespace Bucktuality.RealtimeService.Services;

public class PresenceClient
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<PresenceClient> _logger;

    public PresenceClient(HttpClient httpClient, ILogger<PresenceClient> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    public async Task SetOnlineAsync(PresenceRequest request)
    {
        await PostAsync("/presence/online", request);
    }

    public async Task SetOfflineAsync(PresenceRequest request)
    {
        await PostAsync("/presence/offline", request);
    }

    public async Task SetStatusAsync(PresenceRequest request)
    {
        await PostAsync("/presence/status", request);
    }

    private async Task PostAsync(string path, PresenceRequest request)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync(path, request);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "Presence call failed. Path={Path}, StatusCode={StatusCode}",
                    path,
                    response.StatusCode
                );
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Presence service unavailable. Path={Path}", path);
        }
    }
}