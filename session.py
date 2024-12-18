from requests import Session
from agents import get_agent

class RandomUserAgentSession(Session):
    """
    Session class which uses a random user agent for each request.
    """
    def request(self, *args, **kwargs):
        self.headers.update({"User-Agent": get_agent()})
        return super().request(*args, **kwargs)
