__version__ = __VERISON__ = "0.0.1.3"
try:
    from .tools import CommandClient
    import fire

    def cmd():
        fire.Fire(CommandClient)


except:
    pass
