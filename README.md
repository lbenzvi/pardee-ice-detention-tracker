# ICE/DHS Contract Ingestion Pipeline

This system polls USAspending for ICE/DHS detention-related contracts, filters for high-signal records, and stores them in Postgres for later analysis and risk modeling.

## Components
- `contracts_pipeline.py` — real-time ingestion
- `backfill_contracts.py` — historical backfill
- `schema.sql` — database schema
- `.env.example` — required environment variables

## Data Source
- USAspending API

## Database
- PostgreSQL
- Main table: `contracts`

## How to Run
1. Set environment variables
2. Run schema.sql
3. Run ingestion script
4. Optionally run backfill script

## Notes
- Upserts are idempotent
- Duplicate award IDs are ignored
- Filtering is tuned for ICE detention-related signal
