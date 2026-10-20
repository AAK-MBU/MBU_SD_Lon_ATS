"""Klient til ServiceNow's API (under udarbejdelse)"""

import logging

logger = logging.getLogger(__name__)


class ServiceNowClient:
    """Opretter sager i ServiceNow.

    Args:
        base_url: ServiceNow-instansens URL.
        username: Brugernavn til API'et.
        password: Adgangskode til API'et.
    """

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password

    def create_case(self, payload: dict) -> str:
        """Opretter en sag i ServiceNow.

        Args:
            payload: Sagens felter, fx short_description, description og
                assignment_group.

        Returns:
            Sagsnummeret på den oprettede sag.
        """
        # TODO: Endpoint, autentificering og felter afklares med ServiceNow
        raise NotImplementedError("Oprettelse af ServiceNow-sager er ikke implementeret endnu")
