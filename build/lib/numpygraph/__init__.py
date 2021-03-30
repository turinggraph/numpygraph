__version__ = __VERISON__ = "0.0.1.3"
from .tools import CommandClient
import fire
def cmd():
    fire.Fire(CommandClient)