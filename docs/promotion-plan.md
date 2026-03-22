# Promotion Plan — Orleans.Streaming.Redis

Action plan for increasing project visibility and adoption.

## 1. Awesome Lists (PRs to submit)

Submit a PR adding this project to each list. Follow each list's contribution guidelines.

| List | URL | Section |
|------|-----|---------|
| awesome-dotnet | https://github.com/quozd/awesome-dotnet | Distributed Computing / Actor Frameworks |
| awesome-dotnet-core | https://github.com/thangchung/awesome-dotnet-core | Frameworks / Actor |
| awesome-redis | https://github.com/JamzyWang/awesome-redis | .NET Libraries or Streaming |
| awesome-orleans | https://github.com/OrleansContrib/awesome-orleans | Stream Providers |
| awesome-csharp | https://github.com/uhub/awesome-c-sharp | Messaging |

**Priority:** awesome-orleans > awesome-dotnet > awesome-redis > rest.

## 2. NuGet Discoverability

- [x] Rich description with keywords (done)
- [x] Tags: orleans, streaming, redis, redis-streams, distributed, actors, event-driven, message-queue, pubsub, dotnet (done)
- [x] README embedded in NuGet package (done)
- [ ] Add icon.png (128x128, Redis + Orleans themed) to repo root
- [ ] Publish to nuget.org with a proper release tag

## 3. Community Posts

### Reddit
- **r/dotnet** — Post: "Orleans.Streaming.Redis — Redis Streams provider for Microsoft Orleans 10.x"
- **r/csharp** — Share as a "Show & Tell" or project showcase
- **r/redis** — Cross-post focusing on the Redis Streams angle

### Hacker News
- Submit as "Show HN: Redis Streams persistent provider for Microsoft Orleans"
- Best time: weekday morning (US Pacific time)

### Dev.to / Hashnode / Medium
- Write an article: "How to Use Redis Streams as a Message Transport for Orleans"
  - Cover: why Redis Streams, architecture, quick setup, benchmarks
  - Tag with: dotnet, redis, orleans, distributed-systems, csharp

### X (Twitter) / Bluesky
- Tweet with hashtags: #dotnet #orleans #redis #opensource #csharp
- Tag @MicrosoftOrleans, @StackExchange (Redis)
- Pin the tweet/post

## 4. Orleans Community

- **Orleans Discord** — Share in the #community-projects or #streaming channel
  - Discord: https://aka.ms/orleans-discord
- **Orleans GitHub Discussions** — Post in dotnet/orleans Discussions as a community project
- **dotnet/orleans issues** — If there's an open issue requesting a Redis stream provider, comment with a link

## 5. GitHub Discoverability

Set these **Topics** on the GitHub repo (Settings → Topics):
```
orleans redis redis-streams dotnet streaming distributed-systems
actor-model event-driven csharp message-queue nuget
```

Enable **GitHub Discussions** on the repo (Settings → Features → Discussions).

Add a **Social Preview image** (Settings → Social Preview):
- 1280x640px, showing: logo + "Redis Streams for Orleans" + key features

Consider adding **GitHub Sponsors** or **Open Collective** if you want to accept donations.

## 6. .NET Community

- **.NET Community Standup** — Suggest to the .NET team for a community links mention
  - They regularly highlight community packages
- **OrleansContrib** — Consider transferring or mirroring to the OrleansContrib GitHub org
  for official community visibility
- **.NET Conf** — Submit a lightning talk proposal for the next .NET Conf

## 7. SEO / Content

- Ensure GitHub repo description is: "Redis Streams persistent stream provider for Microsoft Orleans 10.x — cross-silo delivery, consumer groups, partitioned queues"
- Blog post on your personal site / company blog
- Answer relevant Stack Overflow questions about Orleans streaming with a mention

## 8. Ongoing

- Respond to issues/PRs quickly (< 24h for first response)
- Tag good first issues for Hacktoberfest (`hacktoberfest` topic in October)
- Publish regular releases with changelogs
- Add a `CHANGELOG.md` with [Keep a Changelog](https://keepachangelog.com/) format
