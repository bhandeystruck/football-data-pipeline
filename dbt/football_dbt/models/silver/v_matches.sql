{{ config(materialized='view') }}

with src as (
  select
    file_key,
    competition_code,
    date_from,
    date_to,
    run_id,
    dt,
    loaded_at,
    payload
  from {{ source('bronze', 'raw_matches') }}
),
flattened as (
  select
    s.file_key,
    s.competition_code,
    s.date_from,
    s.date_to,
    s.run_id,
    s.dt,
    s.loaded_at,

    m.value:id::number                as match_id,
    m.value:utcDate::timestamp_tz     as utc_date,
    m.value:status::string            as status,
    m.value:stage::string             as stage,
    m.value:group::string             as match_group,
    m.value:matchday::number          as matchday,
    m.value:lastUpdated::timestamp_tz as last_updated,

    m.value:competition:id::number    as competition_id,
    m.value:competition:code::string  as competition_code_in_payload,
    m.value:competition:name::string  as competition_name,
    m.value:competition:type::string  as competition_type,

    m.value:season:id::number              as season_id,
    m.value:season:startDate::date         as season_start_date,
    m.value:season:endDate::date           as season_end_date,
    m.value:season:currentMatchday::number as season_current_matchday,

    m.value:homeTeam:id::number        as home_team_id,
    m.value:homeTeam:name::string      as home_team_name,
    m.value:homeTeam:shortName::string as home_team_short_name,
    m.value:homeTeam:tla::string       as home_team_tla,
    m.value:homeTeam:crest::string     as home_team_crest,

    m.value:awayTeam:id::number        as away_team_id,
    m.value:awayTeam:name::string      as away_team_name,
    m.value:awayTeam:shortName::string as away_team_short_name,
    m.value:awayTeam:tla::string       as away_team_tla,
    m.value:awayTeam:crest::string     as away_team_crest,

    m.value:score:winner::string        as winner,
    m.value:score:duration::string      as duration,
    m.value:score:fullTime:home::number as ft_home_goals,
    m.value:score:fullTime:away::number as ft_away_goals,
    m.value:score:halfTime:home::number as ht_home_goals,
    m.value:score:halfTime:away::number as ht_away_goals

  from src s,
  lateral flatten(input => s.payload:matches) m
)

select * from flattened