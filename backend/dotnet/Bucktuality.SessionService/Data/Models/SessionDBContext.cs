using Bucktuality.SessionService.Models;
using Microsoft.EntityFrameworkCore;

namespace Bucktuality.SessionService.Data;

public class SessionDbContext : DbContext
{
    public SessionDbContext(DbContextOptions<SessionDbContext> options)
        : base(options)
    {
    }

    public DbSet<ChatSession> ChatSessions => Set<ChatSession>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ChatSession>(entity =>
        {
            entity.HasKey(x => x.Id);

            entity.Property(x => x.RoomId)
                .HasMaxLength(200)
                .IsRequired();

            entity.Property(x => x.User1Id)
                .HasMaxLength(100)
                .IsRequired();

            entity.Property(x => x.User2Id)
                .HasMaxLength(100)
                .IsRequired();

            entity.Property(x => x.Status)
                .HasMaxLength(50)
                .IsRequired();

            entity.HasIndex(x => x.RoomId);
        });
    }
}