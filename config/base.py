from pydantic import BaseSettings


class Config(BaseSettings):
    BOOTSTRAP_SERVER: str
    TOPIC: str
    TOPIC_RESULT_DEMOGRAPHY: str
    GROUP_ID: str
    AUTO_OFFSET_RESET: str
    API_DEMOGRAPHY_URL: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


app_setting = Config()
