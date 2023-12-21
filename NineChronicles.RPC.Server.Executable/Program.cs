using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.EntityFrameworkCore;
using Nekoyume.Shared.Hubs;
using NineChronicles.RPC.Server.Executable;
using NineChronicles.RPC.Server.Executable.Hubs;
using NineChronicles.RPC.Server.Executable.Options;
using NineChronicles.RPC.Server.Store;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<HeadlessConnectionOption>(builder.Configuration.GetSection("HeadlessConnection"));
builder.WebHost.ConfigureKestrel(options =>
{
    // WORKAROUND: Accept HTTP/2 only to allow insecure HTTP/2 connections during development.
    options.ListenAnyIP(5250, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;
    });
});

builder.Host.ConfigureLogging(logging =>
{
    logging.ClearProviders();
    logging.AddConsole();
});

builder.Host.ConfigureServices(services =>
{
    var mysqlConnectionString = builder.Configuration.GetSection("MySqlConnectionString").Value;
    services.AddDbContextFactory<NineChroniclesContext>(options =>
    {
        if (mysqlConnectionString != string.Empty)
        {
            args = new[] { mysqlConnectionString }!;
        }

        if (args.Length == 1)
        {
            options.UseMySql(
                args[0],
                ServerVersion.AutoDetect(
                    args[0]),
                b => b.MigrationsAssembly("NineChronicles.DataProvider.Executable"));
        }
        else
        {
            options.UseSqlite(
                @"Data Source=9c.gg.db",
                b => b.MigrationsAssembly("NineChronicles.DataProvider.Executable"));
        }
    });
    services.AddDbContextFactory<NineChroniclesContext>(
        options => options.UseMySql(
            mysqlConnectionString!,
            ServerVersion.AutoDetect(mysqlConnectionString),
            mySqlOptions =>
            {
                mySqlOptions
                    .EnableRetryOnFailure(
                        maxRetryCount: 20,
                        maxRetryDelay: TimeSpan.FromSeconds(10),
                        errorNumbersToAdd: null);
            }));
});

builder.Services.AddGrpc();
builder.Services.AddMagicOnion();

builder.Services.AddSingleton<IActionEvaluationHub, ActionEvaluationHub>();
builder.Services.AddSingleton<IStateCache, StateCache>();
builder.Services.AddSingleton<MySqlStore>();
builder.Services.AddHostedService<HeadlessHubReceiver>();

var app = builder.Build();

app.UseRouting();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
}

app.MapMagicOnionService();

app.Run();