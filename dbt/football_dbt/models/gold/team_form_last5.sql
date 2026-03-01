{{ config(materialized='table') }}

with finished as (

  select
    competition_code,
    utc_date,
    home_team_id as team_id,
    home_team_name as team_name,
    ft_home_goals as gf,
    ft_away_goals as ga,
    case
      when winner = 'HOME_TEAM' then 'W'
      when winner = 'DRAW' then 'D'
      when winner = 'AWAY_TEAM' then 'L'
      else null
    end as result
  from {{ ref('matches_latest') }}
  where status = 'FINISHED' and home_team_id is not null

  union all

  select
    competition_code,
    utc_date,
    away_team_id as team_id,
    away_team_name as team_name,
    ft_away_goals as gf,
    ft_home_goals as ga,
    case
      when winner = 'AWAY_TEAM' then 'W'
      when winner = 'DRAW' then 'D'
      when winner = 'HOME_TEAM' then 'L'
      else null
    end as result
  from {{ ref('matches_latest') }}
  where status = 'FINISHED' and away_team_id is not null
),

ranked as (
  select
    *,
    row_number() over (
      partition by competition_code, team_id
      order by utc_date desc
    ) as rn
  from finished
),

last5 as (
  select * from ranked where rn <= 5
)

select
  competition_code,
  team_id,
  max(team_name) as team_name,

  count(*) as games_played_last5,
  sum(iff(result = 'W', 1, 0)) as wins_last5,
  sum(iff(result = 'D', 1, 0)) as draws_last5,
  sum(iff(result = 'L', 1, 0)) as losses_last5,

  sum(gf) as goals_for_last5,
  sum(ga) as goals_against_last5,
  sum(gf) - sum(ga) as goal_diff_last5,

  sum(case when result = 'W' then 3 when result = 'D' then 1 else 0 end) as points_last5,
  max(utc_date) as last_match_utc

from last5
group by competition_code, team_id