"""FastAPI program - Chapter 4"""
from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session
from datetime import date

import crud, schemas
from database import SessionLocal

app = FastAPI()

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
async def root():
    return {"message": "API health check successful"}


@app.get("/v0/players/", response_model=list[schemas.Player])
def read_players(skip: int = 0, limit: int = 100, minimum_last_changed_date: date = None, first_name: str = None, last_name: str = None,db: Session = Depends(get_db)):
    players = crud.get_players(db, skip=skip, limit=limit, min_last_changed_date=minimum_last_changed_date, first_name=first_name, last_name=last_name)
    return players


@app.get("/v0/players/{player_id}", response_model=schemas.Player)
def read_player(player_id: int, db: Session = Depends(get_db)):
    player = crud.get_player(db, player_id=player_id)
    if player is None:
        raise HTTPException(status_code=404, detail="Player not found")
    return player

@app.get("/v0/performances/", response_model=list[schemas.Performance])
def read_performances(skip: int = 0, limit: int = 100, minimum_last_changed_date: date = None, db: Session = Depends(get_db)):
    performances = crud.get_performances(db, skip=skip, limit=limit, min_last_changed_date=minimum_last_changed_date)
    return performances

@app.get("/v0/leagues/{league_id}", response_model=schemas.League)
def read_league(league_id: int,db: Session = Depends(get_db)):
    league = crud.get_league(db, league_id = league_id)
    if league is None:
        raise HTTPException(status_code=404, detail="League not found")
    return league


@app.get("/v0/leagues/", response_model=list[schemas.League])
def read_leagues(skip: int = 0, limit: int = 100, minimum_last_changed_date: date = None, league_name: str = None,db: Session = Depends(get_db)):
    leagues = crud.get_leagues(db, skip=skip, limit=limit, min_last_changed_date=minimum_last_changed_date, league_name=league_name)
    return leagues

@app.get("/v0/teams/", response_model=list[schemas.Team])
def read_teams(skip: int = 0, limit: int = 100, minimum_last_changed_date: date = None, team_name: str = None, league_id: int = None, db: Session = Depends(get_db)):
    teams = crud.get_teams(db, skip=skip, limit=limit, min_last_changed_date=minimum_last_changed_date, team_name=team_name, league_id=league_id)
    return teams


@app.get("/v0/counts/", response_model=schemas.Counts)
def get_count(db: Session = Depends(get_db)):
    counts = schemas.Counts(
        league_count = crud.get_league_count(db),
        team_count = crud.get_team_count(db),
        player_count = crud.get_player_count(db))
    return counts
