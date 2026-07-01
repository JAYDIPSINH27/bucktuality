using Bucktuality.ModerationService.Models;
using Microsoft.EntityFrameworkCore;

namespace Bucktuality.ModerationService.Data;

public class ModerationDbContext : DbContext
{
    public ModerationDbContext(DbContextOptions<ModerationDbContext> options)
        : base(options)
    {
    }

    public DbSet<Report> Reports => Set<Report>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Report>(entity =>
        {
            entity.HasKey(x => x.Id);

            entity.Property(x => x.RoomId)
                .HasMaxLength(200)
                .IsRequired();

            entity.Property(x => x.ReporterUserId)
                .HasMaxLength(100)
                .IsRequired();

            entity.Property(x => x.ReportedUserId)
                .HasMaxLength(100)
                .IsRequired();

            entity.Property(x => x.Reason)
                .HasMaxLength(500)
                .IsRequired();

            entity.HasIndex(x => x.RoomId);
            entity.HasIndex(x => x.ReportedUserId);
            entity.HasIndex(x => x.CreatedAtUtc);
        });
    }
}