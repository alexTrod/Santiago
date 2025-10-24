
useful command : 

run container 
```console
docker container run -e POSTGRES_PASSWORD=XXX timescale/timescaledb-ha:pg17 
```

connect to postgres

```console
sudo -u postgres psql
``` 
Database:Gulf (\c gulf to connect to it in psql)

### features  
- [ ] insider trading detection & investigation
- [ ] tracking whales 
- [ ] social media sentiment analysis per market/outcome
- [ ] market gap analysis for arbitrage opportunity

### tech stack mvp 

- [ ] grafana 
- [ ] TimeScaleDB (with PostgreSQL)
- [ ] maybe redis
___

### next step
- [ ] data ingestion
- check whatsup with polymarket API (only retrieving xx markets for now)
- [X] fix vps exposure to access postgresql
- [ ] everything else

*data ingestion*
- [ ] remove crypto and sports
- [ ] retrieve data market per market -> order market according to whatever we want (most volumes first / some specific categories)
___
- [ ] get a network of slugs
- events > markets