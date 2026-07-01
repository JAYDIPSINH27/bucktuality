using Bucktuality.UserService.Models;
using Microsoft.EntityFrameworkCore;

namespace Bucktuality.UserService.Data;

public class UserDbContext : DbContext
{
    public UserDbContext(DbContextOptions<UserDbContext> options)
        : base(options)
    {
    }

    public DbSet<User> Users => Set<User>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(entity =>
        {
            entity.HasKey(x => x.Id);

            entity.Property(x => x.DisplayName)
                .HasMaxLength(100)
                .IsRequired();

            entity.Property(x => x.Vibe)
                .HasMaxLength(100);
        });
    }
}