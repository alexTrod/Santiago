
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
check whatsup with polymarket API (only retrieving 941 markets for now)
- [X] fix vps exposure to access postgresql
- [ ] everything else
