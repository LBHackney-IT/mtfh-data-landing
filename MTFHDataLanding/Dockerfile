FROM mcr.microsoft.com/dotnet/core/sdk:3.1

ARG LBHPACKAGESTOKEN
ENV LBHPACKAGESTOKEN=$LBHPACKAGESTOKEN
WORKDIR /app

# Copy csproj and NUGet.Config and restore as distinct layers
COPY ./MTFHDataLanding/MTFHDataLanding.csproj ./
COPY /NuGet.Config /root/.nuget/NuGet/NuGet.Config
RUN dotnet restore ./MTFHDataLanding.csproj


# Copy everything else and build
COPY ./MTFHDataLanding ./
RUN dotnet build -c Release -o out ./MTFHDataLanding.csproj
RUN dotnet publish -c Release -o out ./MTFHDataLanding.csproj

EXPOSE ${PORT:-3000}
CMD ASPNETCORE_URLS=http://+:${PORT:-3000} dotnet ./out/MTFHDataLanding.dll
