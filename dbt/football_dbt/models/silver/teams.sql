{{ config(
    materialized='incremental',
    unique_key='team_id',
    incremental_strategy='merge'
) }}

with base as (
  select
    competition_code,
    season_id,
    utc_date,

    home_team_id as team_id,
    home_team_name as name,
    home_team_short_name as short_name,
    home_team_tla as tla,
    home_team_crest as crest
  from {{ ref('matches_latest') }}
  where home_team_id is not null

  union all

  select
    competition_code,
    season_id,
    utc_date,

    away_team_id as team_id,
    away_team_name as name,
    away_team_short_name as short_name,
    away_team_tla as tla,
    away_team_crest as crest
  from {{ ref('matches_latest') }}
  where away_team_id is not null
),

ranked as (
  select
    *,
    row_number() over (
      partition by team_id
      order by utc_date desc
    ) as rn
  from base
)

select
  team_id,
  name,
  short_name,
  tla,
  crest,
  competition_code,
  season_id,
  utc_date as last_seen_utc_date,
  current_timestamp() as updated_at
from ranked
where rn = 1