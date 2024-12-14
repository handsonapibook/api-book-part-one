import httpx
import swcpy.swc_config as config
from .schemas import League, Team, Player, Performance, Counts
from typing import List
import backoff
import logging
logger = logging.getLogger(__name__)

class SWCClient:
    """Interacts with the Sports World Central API.

        This SDK class simplifies the process of using the SWC fantasy
        football API. It supports all the functions of SWC API and returns
        validated datatypes.

    Typical usage example:

        client = SWCClient()
        response = client.get_health_check()

    """

    HEALTH_CHECK_ENDPOINT = "/"
    LIST_LEAGUES_ENDPOINT = "/v0/leagues/"
    LIST_PLAYERS_ENDPOINT = "/v0/players/"
    LIST_PERFORMANCES_ENDPOINT = "/v0/performances/"
    LIST_TEAMS_ENDPOINT = "/v0/teams/"
    GET_COUNTS_ENDPOINT = "/v0/counts/"

    BULK_FILE_BASE_URL = (
        "https://raw.githubusercontent.com/[github ID]"
        + "/portfolio-project/main/bulk/"
    )



    def __init__(self, input_config: config.SWCConfig):
        """Class constructor that sets varibles from configuration object."""

        logger.debug(f"Bulk file base URL: {self.BULK_FILE_BASE_URL}")

        logger.debug(f"Input config: {input_config}")

        self.swc_base_url = input_config.swc_base_url
        self.backoff = input_config.swc_backoff
        self.backoff_max_time = input_config.swc_backoff_max_time
        self.bulk_file_format = input_config.swc_bulk_file_format

        self.BULK_FILE_NAMES = {
            "players": "player_data",
            "leagues": "league_data",
            "performances": "performance_data",
            "teams": "team_data",
            "team_players": "team_player_data",
        }

        if self.backoff:
            self.call_api = backoff.on_exception(
                wait_gen=backoff.expo,
                exception=(httpx.RequestError, httpx.HTTPStatusError),
                max_time=self.backoff_max_time,
                jitter=backoff.random_jitter,
            )(self.call_api)

        if self.bulk_file_format.lower() == "parquet":
            self.BULK_FILE_NAMES = {
                key: value + ".parquet" for key, value in self.BULK_FILE_NAMES.items()
            }
        else:
            self.BULK_FILE_NAMES = {
                key: value + ".csv" for key, value in self.BULK_FILE_NAMES.items()
            }

        logger.debug(f"Bulk file dictionary: {self.BULK_FILE_NAMES}")

    def call_api(self,
                api_endpoint: str,
                api_params: dict = None
            ) -> httpx.Response:
            """Makes API call and logs errors."""

            if api_params:
                api_params = {key: val for key, val in api_params.items() if val is not None}

            try:
                with httpx.Client(base_url=self.swc_base_url) as client: 
                    logger.debug(f"base_url: {self.swc_base_url}, api_endpoint: {api_endpoint}, api_params: {api_params}")
                    response = client.get(api_endpoint, params=api_params)
                    logger.debug(f"Response JSON: {response.json()}")
                    return response
            except httpx.HTTPStatusError as e:
                logger.error(
                    f"HTTP status error occurred: {e.response.status_code} {e.response.text}"
                )
                raise
            except httpx.RequestError as e:
                logger.error(f"Request error occurred: {str(e)}")
                raise

    def get_health_check(self) -> httpx.Response:
        """Checks if API is running and healthy.

        Calls the API health check endpoint and returns a standard
        message if the API is running normally. Can be used to check
        status of API before making more complicated API calls.

        Returns:
            An httpx.Response object that contains the HTTP status,
            JSON response and other information received from the API.

        """
        logger.debug("Entered health check")
        endpoint_url = self.HEALTH_CHECK_ENDPOINT
        return self.call_api(endpoint_url)

    def list_leagues(
        self,
        skip: int = 0, 
        limit: int = 100, 
        minimum_last_changed_date: str = None,
        league_name: str = None,
    ) -> List[League]:
        """Returns a List of Leagues filtered by parameters.

        Calls the API v0/leagues endpoint and returns a list of
        League objects.

        Returns:
        A List of schemas.League objects. Each represents one
        SportsWorldCentral fantasy league.

        """
        logger.debug("Entered list leagues")

        params = { 
            "skip": skip,
            "limit": limit,
            "minimum_last_changed_date": minimum_last_changed_date,
            "league_name": league_name,
        }

        response = self.call_api(self.LIST_LEAGUES_ENDPOINT, params)
        return [League(**league) for league in response.json()]
    
        
    def get_league_by_id(self, league_id: int) -> League:
        """Returns a Leagues matching a league_id.

        Calls the API v0/leagues/{league_id} endpoint and returns single League.

        Returns:
        A schemas.League objects representing one SportsWorldCentral fantasy league.

        """
        logger.debug("Entered get league by ID")
        # build URL
        endpoint_url = f"{self.LIST_LEAGUES_ENDPOINT}{league_id}"
        # make the API call
        response = self.call_api(endpoint_url)
        responseLeague = League(**response.json())
        return responseLeague


    def get_counts(self) -> Counts:
        """Returns Counts of several endpoints.

        Calls the API v0/counts endpoint and returns a Counts object.

        Returns:
        Counts object representing counts of elements in the API.,

        """
        logger.debug("Entered get counts")

        response = self.call_api(self.GET_COUNTS_ENDPOINT)
        responseCounts = Counts(**response.json())
        return responseCounts

    def list_teams(
        self,
        skip: int = 0,
        limit: int = 100,
        minimum_last_changed_date: str = None,
        team_name: str = None,
        league_id: int = None,
    ):
        """Returns a List of Teams filtered by parameters.

        Calls the API v0/teams endpoint and returns a list of
        Team objects.

        Returns:
        A List of schemas.Team objects. Each represents one
        team in a SportsWorldCentral fantasy league.

        """

        logger.debug("Entered list teams")

        params = {
            "skip": skip,
            "limit": limit,
            "minimum_last_changed_date": minimum_last_changed_date,
            "team_name": team_name,
            "league_id": league_id,
        }
        response = self.call_api(self.LIST_TEAMS_ENDPOINT, params)
        return [Team(**team) for team in response.json()]

    def list_players(
        self,
        skip: int = 0,
        limit: int = 100,
        minimum_last_changed_date: str = None,
        first_name: str = None,
        last_name: str = None,
    ):
        """Returns a List of Players filtered by parameters.

        Calls the API v0/players endpoint and returns a list of
        Player objects.

        Returns:
        A List of schemas.Player objects. Each represents one
        NFL player.

        """
        logger.debug("Entered list players")

        params = {
            "skip": skip,
            "limit": limit,
            "minimum_last_changed_date": minimum_last_changed_date,
            "first_name": first_name,
            "last_name": last_name,
        }

        response = self.call_api(self.LIST_PLAYERS_ENDPOINT, params)
        return [Player(**player) for player in response.json()]

    def get_player_by_id(self, player_id: int):
        """Returns a Players matching the SWC Player ID.

        Calls the API v0/players/{player_id} endpoint and returns
        one Player pboject.

        Returns:
        A List of schemas.Player object representing an
        NFL player.

        """
        logger.debug("Entered get player by ID")

        # build URL
        endpoint_url = f"{self.LIST_PLAYERS_ENDPOINT}{player_id}"
        # make the API call
        response = self.call_api(endpoint_url)
        responsePlayer = Player(**response.json())
        return responsePlayer

    def list_performances(
        self, skip: int = 0, limit: int = 100, minimum_last_changed_date: str = None
    ):
        """Returns a List of Performances filtered by parameters.

        Calls the API v0/performances endpoint and returns a list of
        Performance objects.

        Returns:
        A List of schemas.Performance objects. Each represents one
        player's scoring for one NFL week.

        """
        logger.debug("Entered get performances")

        params = {
            "skip": skip,
            "limit": limit,
            "minimum_last_changed_date": minimum_last_changed_date,
        }

        response = self.call_api(self.LIST_PERFORMANCES_ENDPOINT, params)
        return [Performance(**peformance) for peformance in response.json()]

#bulk endpoints

    def get_bulk_player_file(self) -> bytes:
        """Returns a bulk file with player data"""

        logger.debug("Entered get bulk player file")

        player_file_path = self.BULK_FILE_BASE_URL + self.BULK_FILE_NAMES["players"]

        response = httpx.get(player_file_path, follow_redirects=True)

        if response.status_code == 200:
            logger.debug("File downloaded successfully")
            return response.content

    def get_bulk_league_file(self) -> bytes:
        """Returns a CSV file with league data"""

        logger.debug("Entered get bulk league file")

        league_file_path = self.BULK_FILE_BASE_URL + self.BULK_FILE_NAMES["leagues"]

        response = httpx.get(league_file_path, follow_redirects=True)

        if response.status_code == 200:
            logger.debug("File downloaded successfully")
            return response.content

    def get_bulk_performance_file(self) -> bytes:
        """Returns a CSV file with performance data"""

        logger.debug("Entered get bulk performance file")

        performance_file_path = (
            self.BULK_FILE_BASE_URL + self.BULK_FILE_NAMES["performances"]
        )

        response = httpx.get(performance_file_path, follow_redirects=True)

        if response.status_code == 200:
            logger.debug("File downloaded successfully")
            return response.content

    def get_bulk_team_file(self) -> bytes:
        """Returns a CSV file with team data"""

        logger.debug("Entered get bulk team file")

        team_file_path = self.BULK_FILE_BASE_URL + self.BULK_FILE_NAMES["teams"]

        response = httpx.get(team_file_path, follow_redirects=True)

        if response.status_code == 200:
            logger.debug("File downloaded successfully")
            return response.content

    def get_bulk_team_player_file(self) -> bytes:
        """Returns a CSV file with team player data"""

        logger.debug("Entered get bulk team player file")

        team_player_file_path = (
            self.BULK_FILE_BASE_URL + self.BULK_FILE_NAMES["team_players"]
        )

        response = httpx.get(team_player_file_path, follow_redirects=True)

        if response.status_code == 200:
            logger.debug("File downloaded successfully")
            return response.content
