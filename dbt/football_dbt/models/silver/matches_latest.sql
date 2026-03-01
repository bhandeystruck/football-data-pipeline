{{ config(
    materialized='incremental',
    unique_key='match_id',
    incremental_strategy='merge'
) }}

with latest_rows as (
  select *
  from {{ ref('v_matches') }}
  qualify row_number() over (
    partition by match_id
    order by loaded_at desc, last_updated desc
  ) = 1
)

select
  match_id,
  utc_date,
  status,
  stage,
  match_group,
  matchday,
  last_updated,

  competition_id,
  competition_code_in_payload as competition_code,
  competition_name,
  competition_type,

  season_id,
  season_start_date,
  season_end_date,
  season_current_matchday,

  home_team_id,
  home_team_name,
  home_team_short_name,
  home_team_tla,
  home_team_crest,

  away_team_id,
  away_team_name,
  away_team_short_name,
  away_team_tla,
  away_team_crest,

  winner,
  duration,
  ft_home_goals,
  ft_away_goals,
  ht_home_goals,
  ht_away_goals,

  file_key,
  competition_code as competition_code_from_run,
  date_from,
  date_to,
  run_id,
  dt,
  loaded_at

from latest_rows