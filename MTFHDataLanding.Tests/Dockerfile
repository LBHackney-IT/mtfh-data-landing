FROM mcr.microsoft.com/dotnet/core/sdk:3.1

# disable microsoft telematry
ENV DOTNET_CLI_TELEMETRY_OPTOUT='true'

ENV DynamoDb_LocalMode='true'
ARG LBHPACKAGESTOKEN
ENV LBHPACKAGESTOKEN=$LBHPACKAGESTOKEN
WORKDIR /app

# Copy csproj and restore as distinct layers
COPY ./MTFHDataLanding.sln ./
COPY ./MTFHDataLanding/MTFHDataLanding.csproj ./MTFHDataLanding/
COPY ./MTFHDataLanding.Tests/MTFHDataLanding.Tests.csproj ./MTFHDataLanding.Tests/
COPY /NuGet.Config /root/.nuget/NuGet/NuGet.Config

RUN dotnet restore ./MTFHDataLanding/MTFHDataLanding.csproj
RUN dotnet restore ./MTFHDataLanding.Tests/MTFHDataLanding.Tests.csproj

# Copy everything else and build
COPY . .

RUN dotnet build -c debug -o out MTFHDataLanding.Tests/MTFHDataLanding.Tests.csproj

CMD dotnet test
