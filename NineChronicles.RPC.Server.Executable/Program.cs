using Microsoft.AspNetCore.Server.Kestrel.Core;
using Nekoyume.Shared.Hubs;
using Nekoyume.Shared.Services;
using NineChronicles.RPC.Server.Executable;
using NineChronicles.RPC.Server.Executable.Hubs;
using NineChronicles.RPC.Server.Executable.Options;
using NineChronicles.RPC.Server.Executable.Services;
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

builder.Services.AddGrpc();
builder.Services.AddMagicOnion();

builder.Services.AddSingleton<IActionEvaluationHub, ActionEvaluationHub>();
builder.Services.AddSingleton<IStateCache, StateCache>();
builder.Services.AddHostedService<HeadlessHubReceiver>();

var app = builder.Build();

app.UseRouting();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
}

app.MapMagicOnionService();

app.Run();