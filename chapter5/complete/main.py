"""FastAPI program - Chapter 5"""

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import date


import crud, schemas
from database import SessionLocal

api_description = """
This API provides read-only access to info from the Sports World Central (SWC) Fantasy Football API. 
The endpoints are grouped into the following categories:

## Analytics
Get information about health of the API and counts of leagues, teams, and players.

## Player
You can get a list of an NFL players, or search for an individual player by player_id.

## Scoring
You can get a list of NFL player performances, including the fantasy points they scored using SWC league scoring.

## Membership
Get information about all the SWC fantasy football leagues and the teams in them.
"""

# FastAPI constructor with additional details added for OpenAPI Specification
app = FastAPI(
    description=api_description,
    title="Sports World Central (SWC) Fantasy Football API",
    version="0.1",
)


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get(
    "/",
    summary="Check to see if the SWC fantasy football API is running",
    description="""Use this endpoint to check if the API is running. You can also check it first before making other calls to be sure it's running.""",
    response_description="A JSON record with a message in it. If the API is running the message will say successful.",
    operation_id="v0_health_check",
    tags=["analytics"],
)
async def root():
    return {"message": "API health check successful"}


@app.get(
    "/v0/players/",
    response_model=list[schemas.Player],
    summary="Get all the SWC players that meet all the parameters you sent with your request",
    description="""Use this endpoint to get a list of SWC players. You can use the parameters to filter down the players in the list. Names are not unique. You use the skip and limit to perform pagination of the API. Don't use the Player ID values to perform counts. Those are not guaranteed to be in order.""",
    response_description="A list of NFL players that are in SWC fantasy football. They don't to be on a team.",
    operation_id="v0_get_players",
    tags=["players"],
)
def read_players(
    skip: int = Query(
        0, description="The number of items to skip at the beginning of API call."
    ),
    limit: int = Query(
        100, description="The number of records to return after the skipped records."
    ),
    minimum_last_changed_date: date = Query(
        None,
        description="The minimum data of change that you want to return records. Exclude any records changed before this.",
    ),
    first_name: str = Query(
        None, description="The first name of the players to return"
    ),
    last_name: str = Query(None, description="The last name of the players to return"),
    db: Session = Depends(get_db),
):
    players = crud.get_players(
        db,
        skip=skip,
        limit=limit,
        min_last_changed_date=minimum_last_changed_date,
        first_name=first_name,
        last_name=last_name,
    )
    return players


@app.get(
    "/v0/players/{player_id}",
    response_model=schemas.Player,
    summary="Get one player using the Player ID, which is internal to SWC",
    description="If you have an SWC Player ID of a player from another API call such as v0_get_players, you can call this API using the player ID",
    response_description="One NFL player",
    operation_id="v0_get_players_by_player_id",
    tags=["players"],
)
def read_player(player_id: int, db: Session = Depends(get_db)):
    player = crud.get_player(db, player_id=player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found")
    return player


@app.get(
    "/v0/performances/",
    response_model=list[schemas.Performance],
    summary="Get all the weekly performances that meet all the parameters you sent with your request",
    description="""Use this endpoint to get lists of weekly performances by players in the SWC. You us the skip and limit to perform pagination of the API. Don't use the Performance ID for counting or logic, because that is an internal ID and is not guaranteed to be sequential""",
    response_description="A list of weekly scoring performances. It may be by multiple players.",
    operation_id="v0_get_performances",
    tags=["scoring"],
)
def read_performances(
    skip: int = Query(
        0, description="The number of items to skip at the beginning of API call."
    ),
    limit: int = Query(
        100, description="The number of records to return after the skipped records."
    ),
    minimum_last_changed_date: date = Query(
        None,
        description="The minimum data of change that you want to return records. Exclude any records changed before this.",
    ),
    db: Session = Depends(get_db),
):
    performances = crud.get_performances(
        db, skip=skip, limit=limit, min_last_changed_date=minimum_last_changed_date
    )
    return performances


@app.get(
    "/v0/leagues/{league_id}",
    response_model=schemas.League,
    summary="Get one league by league id",
    description="""Use this endpoint to get a single league that matches the league ID provided by the user.""",
    response_description="An SWC league",
    operation_id="v0_get_league_by_league_id",
    tags=["membership"],
)
def read_league(league_id: int, db: Session = Depends(get_db)):
    league = crud.get_league(db, league_id=league_id)
    if league is None:
        raise HTTPException(status_code=404, detail="League not found")
    return league


@app.get(
    "/v0/leagues/",
    response_model=list[schemas.League],
    summary="Get all the SWC fantasy football leagues that match the parameters you send",
    description="""Use this endpoint to get lists of SWC fantasy football leagues. You us the skip and limit to perform pagination of the API. League name is not guaranteed to be unique. Don't use the League ID for counting or logic, because that is an internal ID and is not guaranteed to be sequential""",
    response_description="A list of leagues on the SWC fantasy football website.",
    operation_id="v0_get_leagues",
    tags=["membership"],
)
def read_leagues(
    skip: int = Query(
        0, description="The number of items to skip at the beginning of API call."
    ),
    limit: int = Query(
        100, description="The number of records to return after the skipped records."
    ),
    minimum_last_changed_date: date = Query(
        None,
        description="The minimum data of change that you want to return records. Exclude any records changed before this.",
    ),
    league_name: str = Query(
        None, description="Name of the leagues to return. Not unique in the SWC."
    ),
    db: Session = Depends(get_db),
):
    leagues = crud.get_leagues(
        db,
        skip=skip,
        limit=limit,
        min_last_changed_date=minimum_last_changed_date,
        league_name=league_name,
    )
    return leagues


@app.get(
    "/v0/teams/",
    response_model=list[schemas.Team],
    summary="Get all the SWC fantasy football teams that match the parameters you send",
    description="""Use this endpoint to get lists of SWC fantasy football teams. You us the skip and limit to perform pagination of the API. Team name is not guaranteed to be unique. If you get the Team ID from another query such as v0_get_players, you can match it with the Team ID from this query.  Don't use the Team ID for counting or logic, because that is an internal ID and is not guaranteed to be sequential""",
    response_description="A list of teams on the SWC fantasy football website.",
    operation_id="v0_get_teams",
    tags=["membership"],
)
def read_teams(
    skip: int = Query(
        0, description="The number of items to skip at the beginning of API call."
    ),
    limit: int = Query(
        100, description="The number of records to return after the skipped records."
    ),
    minimum_last_changed_date: date = Query(
        None,
        description="The minimum data of change that you want to return records. Exclude any records changed before this.",
    ),
    team_name: str = Query(
        None,
        description="Name of the teams to return. Not unique across SWC, but is unique inside a league.",
    ),
    league_id: int = Query(
        None, description="League ID of the teams to return. Unique in SWC."
    ),
    db: Session = Depends(get_db),
):
    teams = crud.get_teams(
        db,
        skip=skip,
        limit=limit,
        min_last_changed_date=minimum_last_changed_date,
        team_name=team_name,
        league_id=league_id,
    )
    return teams


@app.get(
    "/v0/counts/",
    response_model=schemas.Counts,
    summary="Get counts of the number of leagues, teams, and players in the SWC fantasy football",
    description="""Use this endpoint to count the number of leagues, teams, and players in the SWC fantasy football. Use in combination with skip and limit in v0_get leagues, v0_get_teams, or v0_get_players. Use this endpoint to get counts instead of making calls to the other APIs.""",
    response_description="A list of teams on the SWC fantasy football website.",
    operation_id="v0_get_counts",
    tags=["analytics"],
)
def get_count(db: Session = Depends(get_db)):
    counts = schemas.Counts(
        league_count=crud.get_league_count(db),
        team_count=crud.get_team_count(db),
        player_count=crud.get_player_count(db),
    )
    return counts
