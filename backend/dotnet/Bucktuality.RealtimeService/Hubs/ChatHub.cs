using Bucktuality.RealtimeService.Services;
using Bucktuality.Shared.Contracts;
using Microsoft.AspNetCore.SignalR;

namespace Bucktuality.RealtimeService.Hubs;

public class ChatHub : Hub
{
    private readonly MatchmakingClient _matchmakingClient;
    private readonly SessionClient _sessionClient;

    private static readonly Dictionary<string, string> ConnectionRooms = new();

    public ChatHub(
        MatchmakingClient matchmakingClient,
        SessionClient sessionClient)
    {
        _matchmakingClient = matchmakingClient;
        _sessionClient = sessionClient;
    }

    public override async Task OnConnectedAsync()
    {
        await Clients.Caller.SendAsync("Connected", new
        {
            connectionId = Context.ConnectionId
        });

        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        string? roomId = null;

        lock (ConnectionRooms)
        {
            if (ConnectionRooms.TryGetValue(Context.ConnectionId, out var existingRoomId))
            {
                roomId = existingRoomId;
                ConnectionRooms.Remove(Context.ConnectionId);
            }
        }

        await _matchmakingClient.LeaveMatchAsync(Context.ConnectionId, roomId);

        if (!string.IsNullOrWhiteSpace(roomId))
        {
            await _sessionClient.EndSessionAsync(roomId);

            await Clients.Group(roomId).SendAsync("PartnerLeft", new
            {
                connectionId = Context.ConnectionId,
                roomId
            });
        }

        await base.OnDisconnectedAsync(exception);
    }

    public async Task StartMatching(string userId, string vibe)
    {
        if (string.IsNullOrWhiteSpace(userId))
        {
            await Clients.Caller.SendAsync("MatchError", new
            {
                message = "User ID is required"
            });

            return;
        }

        var request = new MatchRequest
        {
            UserId = userId,
            ConnectionId = Context.ConnectionId,
            Vibe = vibe
        };

        var result = await _matchmakingClient.StartMatchAsync(request);

        if (result == null)
        {
            await Clients.Caller.SendAsync("MatchError", new
            {
                message = "Matchmaking service unavailable"
            });

            return;
        }

        if (!result.IsMatched)
        {
            await Clients.Caller.SendAsync("WaitingForMatch", new
            {
                status = "waiting"
            });

            return;
        }

        var roomId = result.RoomId!;

        await Groups.AddToGroupAsync(Context.ConnectionId, roomId);

        if (!string.IsNullOrWhiteSpace(result.PartnerConnectionId))
        {
            await Groups.AddToGroupAsync(result.PartnerConnectionId, roomId);
        }

        lock (ConnectionRooms)
        {
            ConnectionRooms[Context.ConnectionId] = roomId;

            if (!string.IsNullOrWhiteSpace(result.PartnerConnectionId))
            {
                ConnectionRooms[result.PartnerConnectionId] = roomId;
            }
        }

        await _sessionClient.CreateSessionAsync(new CreateSessionRequest
        {
            RoomId = roomId,
            User1Id = userId,
            User2Id = result.PartnerUserId ?? "unknown"
        });

        await Clients.Client(Context.ConnectionId).SendAsync("MatchFound", result);

        if (!string.IsNullOrWhiteSpace(result.PartnerConnectionId))
        {
            await Clients.Client(result.PartnerConnectionId).SendAsync("MatchFound", new
            {
                isMatched = true,
                status = "matched",
                roomId,
                partnerUserId = userId,
                partnerConnectionId = Context.ConnectionId
            });
        }
    }

    public async Task SendMessage(string roomId, string userId, string message)
    {
        if (string.IsNullOrWhiteSpace(roomId) || string.IsNullOrWhiteSpace(message))
        {
            return;
        }

        var chatMessage = new ChatMessage
        {
            RoomId = roomId,
            SenderUserId = userId,
            Message = message,
            SentAt = DateTime.UtcNow
        };

        await Clients.Group(roomId).SendAsync("ReceiveMessage", chatMessage);
    }

    public async Task LeaveRoom(string roomId)
    {
        if (string.IsNullOrWhiteSpace(roomId))
        {
            return;
        }

        await Groups.RemoveFromGroupAsync(Context.ConnectionId, roomId);

        lock (ConnectionRooms)
        {
            ConnectionRooms.Remove(Context.ConnectionId);
        }

        await _matchmakingClient.LeaveMatchAsync(Context.ConnectionId, roomId);
        await _sessionClient.EndSessionAsync(roomId);

        await Clients.Caller.SendAsync("LeftRoom", new
        {
            roomId,
            status = "left"
        });

        await Clients.Group(roomId).SendAsync("PartnerLeft", new
        {
            connectionId = Context.ConnectionId,
            roomId
        });
    }

    public async Task SendOffer(string roomId, string offer)
{
    if (string.IsNullOrWhiteSpace(roomId) || string.IsNullOrWhiteSpace(offer))
    {
        return;
    }

    await Clients.OthersInGroup(roomId).SendAsync("ReceiveOffer", new
    {
        roomId,
        offer
    });
}

public async Task SendAnswer(string roomId, string answer)
{
    if (string.IsNullOrWhiteSpace(roomId) || string.IsNullOrWhiteSpace(answer))
    {
        return;
    }

    await Clients.OthersInGroup(roomId).SendAsync("ReceiveAnswer", new
    {
        roomId,
        answer
    });
}

public async Task SendIceCandidate(string roomId, string candidate)
{
    if (string.IsNullOrWhiteSpace(roomId) || string.IsNullOrWhiteSpace(candidate))
    {
        return;
    }

    await Clients.OthersInGroup(roomId).SendAsync("ReceiveIceCandidate", new
    {
        roomId,
        candidate
    });
}
}