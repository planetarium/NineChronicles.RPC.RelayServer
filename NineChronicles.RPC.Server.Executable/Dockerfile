FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["NineChronicles.RPC.Server.Executable/NineChronicles.RPC.Server.Executable.csproj", "NineChronicles.RPC.Server.Executable/"]
RUN dotnet restore "NineChronicles.RPC.Server.Executable/NineChronicles.RPC.Server.Executable.csproj"
COPY . .
WORKDIR "/src/NineChronicles.RPC.Server.Executable"
RUN dotnet build "NineChronicles.RPC.Server.Executable.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "NineChronicles.RPC.Server.Executable.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "NineChronicles.RPC.Server.Executable.dll"]
