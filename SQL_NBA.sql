-- ============================================================
--  NBA Star Schema
--  Baza: MS SQL Server 2022  |  Database: NBA
--  Tabela faktów: fact_player_game_stats
-- ============================================================
--
--              dim_date
--                 |
--   dim_team ── fact_player_game_stats ── dim_player
--                 |
--              dim_game
--
--  Tabele źródłowe (surowe dane z Kaggle):
--    players        : TEAM_ID, PLAYER_ID, SEASON
--    teams          : TEAM_ID, ABBREVIATION, NICKNAME, CITY, ARENA,
--                     ARENACAPACITY, YEARFOUNDED
--    games          : GAME_DATE_EST, GAME_ID, GAME_STATUS_TEXT,
--                     HOME_TEAM_ID, VISITOR_TEAM_ID, SEASON,
--                     HOME_TEAM_WINS, PTS_home, PTS_away, ...
--    games_details  : GAME_ID, TEAM_ID, TEAM_ABBREVIATION, TEAM_CITY,
--                     PLAYER_ID, PLAYER_NAME, NICKNAME, START_POSITION,
--                     COMMENT, MIN, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT,
--                     FTM, FTA, FT_PCT, OREB, DREB, REB, AST, STL, BLK,
--                     TO, PF, PTS, PLUS_MINUS
--    ranking        : TEAM_ID, LEAGUE_ID, SEASON_ID, STANDINGSDATE,
--                     CONFERENCE, TEAM, G, W, L, W_PCT,
--                     HOME_RECORD, ROAD_RECORD, RETURNTOPLAY
-- ============================================================

USE NBA;
GO

-- schemat dw oddziela tabele hurtowni od surowych danych
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw');
GO


-- ============================================================
-- 1. WYMIARY
-- ============================================================

-- fakty usuwamy pierwsze, bo mają FK do wszystkich wymiarów
DROP TABLE IF EXISTS dw.fact_player_game_stats;
GO

-- ------------------------------------------------------------
-- dim_player
-- uwaga: players.csv nie zawiera kolumny z nazwiskiem gracza
-- nazwisko bierzemy z games_details (pojawia się przy każdym meczu)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS dw.dim_player;
GO
CREATE TABLE dw.dim_player (
    player_sk    INT IDENTITY(1,1) NOT NULL,
    player_id    INT               NOT NULL,
    player_name  NVARCHAR(100)     NOT NULL,
    team_id      INT                   NULL,
    CONSTRAINT PK_dim_player    PRIMARY KEY (player_sk),
    CONSTRAINT UQ_dim_player_id UNIQUE      (player_id)
);
GO

INSERT INTO dw.dim_player (player_id, player_name, team_id)
SELECT
    PLAYER_ID,
    MAX(PLAYER_NAME),
    MAX(TEAM_ID)
FROM NBA.dbo.games_details
WHERE PLAYER_ID IS NOT NULL
GROUP BY PLAYER_ID;
GO


-- ------------------------------------------------------------
-- dim_team
-- ------------------------------------------------------------
DROP TABLE IF EXISTS dw.dim_team;
GO
CREATE TABLE dw.dim_team (
    team_sk        INT IDENTITY(1,1) NOT NULL,
    team_id        INT               NOT NULL,
    abbreviation   NVARCHAR(10)          NULL,
    nickname       NVARCHAR(100)         NULL,
    city           NVARCHAR(100)         NULL,
    arena          NVARCHAR(100)         NULL,
    arena_capacity INT                   NULL,
    year_founded   INT                   NULL,
    CONSTRAINT PK_dim_team    PRIMARY KEY (team_sk),
    CONSTRAINT UQ_dim_team_id UNIQUE      (team_id)
);
GO

INSERT INTO dw.dim_team (team_id, abbreviation, nickname, city, arena, arena_capacity, year_founded)
SELECT
    TEAM_ID, ABBREVIATION, NICKNAME, CITY, ARENA, ARENACAPACITY, YEARFOUNDED
FROM NBA.dbo.teams;
GO


-- ------------------------------------------------------------
-- dim_game
-- problem: SQL Server zaimportował GAME_ID jako typ TIME
-- zamiast INT (np. '22:20:04.77' zamiast 22200477)
-- konwersja: usuwamy ':' i '.', dzielimy przez 100000
-- WHERE usuwa ~12k pustych wierszy z importu CSV
-- ------------------------------------------------------------
DROP TABLE IF EXISTS dw.dim_game;
GO
CREATE TABLE dw.dim_game (
    game_sk          INT IDENTITY(1,1) NOT NULL,
    game_id          NVARCHAR(20)      NOT NULL,
    season           INT                   NULL,
    home_team_id     INT                   NULL,
    visitor_team_id  INT                   NULL,
    game_status_text NVARCHAR(50)          NULL,
    home_team_wins   TINYINT               NULL,
    pts_home         SMALLINT              NULL,
    pts_away         SMALLINT              NULL,
    CONSTRAINT PK_dim_game    PRIMARY KEY (game_sk),
    CONSTRAINT UQ_dim_game_id UNIQUE      (game_id)
);
GO

INSERT INTO dw.dim_game
    (game_id, season, home_team_id, visitor_team_id,
     game_status_text, home_team_wins, pts_home, pts_away)
SELECT DISTINCT
    CAST(
        CAST(REPLACE(REPLACE(CONVERT(VARCHAR(20), GAME_ID, 114), ':', ''), '.', '') AS BIGINT)
        / 100000
    AS INT),
    SEASON,
    HOME_TEAM_ID,
    VISITOR_TEAM_ID,
    GAME_STATUS_TEXT,
    TRY_CAST(HOME_TEAM_WINS AS TINYINT),
    TRY_CAST(PTS_home       AS SMALLINT),
    TRY_CAST(PTS_away       AS SMALLINT)
FROM NBA.dbo.games
WHERE GAME_ID IS NOT NULL;
GO


-- ------------------------------------------------------------
-- dim_date
-- generujemy z GAME_DATE_EST zamiast ładować z zewnątrz
-- nba_season_label: sezon zaczyna się w październiku,
-- więc miesiące 10-12 należą do sezonu roku następnego
-- ------------------------------------------------------------
DROP TABLE IF EXISTS dw.dim_date;
GO
CREATE TABLE dw.dim_date (
    date_sk          INT          NOT NULL,   -- format YYYYMMDD
    full_date        DATE         NOT NULL,
    year             SMALLINT     NOT NULL,
    quarter          TINYINT      NOT NULL,
    month            TINYINT      NOT NULL,
    month_name       NVARCHAR(20) NOT NULL,
    day_of_month     TINYINT      NOT NULL,
    day_of_week      TINYINT      NOT NULL,
    day_name         NVARCHAR(20) NOT NULL,
    is_weekend       BIT          NOT NULL,
    nba_season_label NVARCHAR(20) NOT NULL,
    CONSTRAINT PK_dim_date PRIMARY KEY (date_sk)
);
GO

WITH dates AS (
    SELECT DISTINCT CAST(GAME_DATE_EST AS DATE) AS game_date
    FROM NBA.dbo.games
    WHERE GAME_DATE_EST IS NOT NULL
)
INSERT INTO dw.dim_date
    (date_sk, full_date, year, quarter, month, month_name,
     day_of_month, day_of_week, day_name, is_weekend, nba_season_label)
SELECT
    CAST(FORMAT(game_date, 'yyyyMMdd') AS INT),
    game_date,
    YEAR(game_date),
    DATEPART(QUARTER, game_date),
    MONTH(game_date),
    DATENAME(MONTH,   game_date),
    DAY(game_date),
    DATEPART(WEEKDAY, game_date),
    DATENAME(WEEKDAY, game_date),
    CASE WHEN DATEPART(WEEKDAY, game_date) IN (1,7) THEN 1 ELSE 0 END,
    CASE
        WHEN MONTH(game_date) >= 10
        THEN CAST(YEAR(game_date)     AS NVARCHAR(4)) + '-' +
             RIGHT(CAST(YEAR(game_date) + 1 AS NVARCHAR(4)), 2)
        ELSE CAST(YEAR(game_date) - 1 AS NVARCHAR(4)) + '-' +
             RIGHT(CAST(YEAR(game_date)     AS NVARCHAR(4)), 2)
    END
FROM dates;
GO


-- ============================================================
-- 2. TABELA FAKTÓW
-- granulacja: 1 wiersz = 1 gracz w 1 meczu
-- ============================================================
CREATE TABLE dw.fact_player_game_stats (
    stat_sk        BIGINT IDENTITY(1,1) NOT NULL,

    player_sk      INT NOT NULL,
    team_sk        INT NOT NULL,
    game_sk        INT NOT NULL,
    date_sk        INT NOT NULL,

    start_position NCHAR(2)      NULL,   -- F/C/G, NULL = rezerwowy
    comment        NVARCHAR(255) NULL,   -- np. 'DNP - Coach Decision'
    minutes_played DECIMAL(5,2)  NULL,

    fgm     SMALLINT     NULL,
    fga     SMALLINT     NULL,
    fg_pct  DECIMAL(5,3) NULL,
    fg3m    SMALLINT     NULL,
    fg3a    SMALLINT     NULL,
    fg3_pct DECIMAL(5,3) NULL,

    ftm    SMALLINT     NULL,
    fta    SMALLINT     NULL,
    ft_pct DECIMAL(5,3) NULL,

    oreb SMALLINT NULL,
    dreb SMALLINT NULL,
    reb  SMALLINT NULL,

    ast         SMALLINT NULL,
    stl         SMALLINT NULL,
    blk         SMALLINT NULL,
    to_turnover SMALLINT NULL,   -- kolumna TO przemianowana, TO to słowo kluczowe w T-SQL
    pf          SMALLINT NULL,
    pts         SMALLINT NULL,
    plus_minus  SMALLINT NULL,

    CONSTRAINT PK_fact        PRIMARY KEY (stat_sk),
    CONSTRAINT FK_fact_player FOREIGN KEY (player_sk) REFERENCES dw.dim_player(player_sk),
    CONSTRAINT FK_fact_team   FOREIGN KEY (team_sk)   REFERENCES dw.dim_team(team_sk),
    CONSTRAINT FK_fact_game   FOREIGN KEY (game_sk)   REFERENCES dw.dim_game(game_sk),
    CONSTRAINT FK_fact_date   FOREIGN KEY (date_sk)   REFERENCES dw.dim_date(date_sk)
);
GO


-- ============================================================
-- 3. ZASILANIE FAKTÓW
-- ============================================================
INSERT INTO dw.fact_player_game_stats (
    player_sk, team_sk, game_sk, date_sk,
    start_position, comment, minutes_played,
    fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
    ftm, fta, ft_pct,
    oreb, dreb, reb,
    ast, stl, blk, to_turnover, pf, pts, plus_minus
)
SELECT
    p.player_sk,
    t.team_sk,
    g.game_sk,
    CAST(FORMAT(CAST(gm.GAME_DATE_EST AS DATE), 'yyyyMMdd') AS INT),

    NULLIF(LTRIM(RTRIM(d.START_POSITION)), ''),
    d.COMMENT,

    -- MIN w źródle to string 'MM:SS', zamieniamy na minuty dziesiętne
    CASE
        WHEN d.MIN LIKE '%:%'
        THEN CAST(LEFT(d.MIN, CHARINDEX(':', d.MIN) - 1) AS DECIMAL(5,2))
           + CAST(RIGHT(d.MIN, LEN(d.MIN) - CHARINDEX(':', d.MIN)) AS DECIMAL(5,2)) / 60.0
        ELSE TRY_CAST(d.MIN AS DECIMAL(5,2))
    END,

    d.FGM, d.FGA, d.FG_PCT,
    d.FG3M, d.FG3A, d.FG3_PCT,
    d.FTM, d.FTA, d.FT_PCT,
    d.OREB, d.DREB, d.REB,
    d.AST, d.STL, d.BLK, d.[TO], d.PF, d.PTS, d.PLUS_MINUS

FROM       NBA.dbo.games_details d
JOIN dw.dim_player  p  ON p.player_id = d.PLAYER_ID
JOIN dw.dim_team    t  ON t.team_id   = d.TEAM_ID
JOIN dw.dim_game    g  ON g.game_id   = d.GAME_ID
-- ta sama konwersja TIME->INT co przy dim_game
JOIN NBA.dbo.games  gm ON CAST(CAST(REPLACE(REPLACE(CONVERT(VARCHAR(20), gm.GAME_ID, 114), ':', ''), '.', '') AS BIGINT) / 100000 AS INT) = d.GAME_ID;
GO


-- ============================================================
-- 4. INDEKSY
-- ============================================================
CREATE NONCLUSTERED INDEX IX_fact_player_sk ON dw.fact_player_game_stats (player_sk);
CREATE NONCLUSTERED INDEX IX_fact_team_sk   ON dw.fact_player_game_stats (team_sk);
CREATE NONCLUSTERED INDEX IX_fact_game_sk   ON dw.fact_player_game_stats (game_sk);
CREATE NONCLUSTERED INDEX IX_fact_date_sk   ON dw.fact_player_game_stats (date_sk);
GO


-- ============================================================
-- 5. WERYFIKACJA
-- ============================================================
SELECT 'dim_player'             AS tabela, COUNT(*) AS wiersze FROM dw.dim_player
UNION ALL
SELECT 'dim_team',                          COUNT(*) FROM dw.dim_team
UNION ALL
SELECT 'dim_game',                          COUNT(*) FROM dw.dim_game
UNION ALL
SELECT 'dim_date',                          COUNT(*) FROM dw.dim_date
UNION ALL
SELECT 'fact_player_game_stats',            COUNT(*) FROM dw.fact_player_game_stats;
GO


-- ============================================================
-- 6. PRZYKŁADOWE ZAPYTANIA
-- ============================================================

-- skuteczność rzutów drużyny per sezon
SELECT
    t.nickname,
    dd.nba_season_label                            AS sezon,
    SUM(f.pts)                                     AS total_pts,
    ROUND(AVG(CAST(f.fg_pct  AS FLOAT)) * 100, 1) AS sr_fg_pct,
    ROUND(AVG(CAST(f.fg3_pct AS FLOAT)) * 100, 1) AS sr_3pt_pct
FROM dw.fact_player_game_stats f
JOIN dw.dim_team t  ON t.team_sk  = f.team_sk
JOIN dw.dim_date dd ON dd.date_sk = f.date_sk
GROUP BY t.nickname, dd.nba_season_label
ORDER BY dd.nba_season_label, total_pts DESC;
GO
