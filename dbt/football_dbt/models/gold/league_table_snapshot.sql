{{ config(materialized='table') }}

with finished as (

  select
    competition_code,
    season_id,
    utc_date,

    home_team_id as team_id,
    home_team_name as team_name,
    ft_home_goals as gf,
    ft_away_goals as ga,
    case
      when winner = 'HOME_TEAM' then 3
      when winner = 'DRAW' then 1
      when winner = 'AWAY_TEAM' then 0
      else 0
    end as pts,
    case when winner = 'HOME_TEAM' then 1 else 0 end as w,
    case when winner = 'DRAW' then 1 else 0 end as d,
    case when winner = 'AWAY_TEAM' then 1 else 0 end as l
  from {{ ref('matches_latest') }}
  where status = 'FINISHED' and home_team_id is not null

  union all

  select
    competition_code,
    season_id,
    utc_date,

    away_team_id as team_id,
    away_team_name as team_name,
    ft_away_goals as gf,
    ft_home_goals as ga,
    case
      when winner = 'AWAY_TEAM' then 3
      when winner = 'DRAW' then 1
      when winner = 'HOME_TEAM' then 0
      else 0
    end as pts,
    case when winner = 'AWAY_TEAM' then 1 else 0 end as w,
    case when winner = 'DRAW' then 1 else 0 end as d,
    case when winner = 'HOME_TEAM' then 1 else 0 end as l
  from {{ ref('matches_latest') }}
  where status = 'FINISHED' and away_team_id is not null
),

agg as (
  select
    competition_code,
    season_id,
    team_id,
    max(team_name) as team_name,
    count(*) as played,
    sum(w) as wins,
    sum(d) as draws,
    sum(l) as losses,
    sum(gf) as goals_for,
    sum(ga) as goals_against,
    sum(gf) - sum(ga) as goal_diff,
    sum(pts) as points,
    max(utc_date) as as_of_utc
  from finished
  group by competition_code, season_id, team_id
),

ranked as (
  select
    *,
    row_number() over (
      partition by competition_code, season_id
      order by points desc, goal_diff desc, goals_for desc, team_name asc
    ) as rank
  from agg
)

select * from ranked