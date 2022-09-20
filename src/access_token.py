import requests


class AccessToken:
    def __init__(self, auth):
        self.auth = auth

    def get_bearer_token(self):
        token = self.extract_token(self.get_token_response())
        return token

    def get_token_response(self):
        url = "https://ssoauthwf.gore.com/as/token.oauth2?grant_type=client_credentials"

        payload = {}
        headers = {"Authorization": self.auth, "Cookie": "PF=zCzfZBOTgHzWFogAbRGjGx"}

        response = requests.request("POST", url, headers=headers, data=payload)

        return response

    def extract_token(self, http_result):
        return http_result.json()["access_token"]
