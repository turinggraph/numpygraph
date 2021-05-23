import fire
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completer, Completion
# from prompt_toolkit.lexers import PygmentsLexer
import click
from fuzzyfinder import fuzzyfinder
# from pygments.lexers import CypherLexer
# from pygments.lexers.html import HtmlLexer
import os
from pathlib import Path

CONFIG_PATH = str(Path.home()) + "/.npg"
SQLKeywords = ["select", "from", "insert", "update", "delete", "drop"]
os.makedirs(CONFIG_PATH, exist_ok=True)


class SQLCompleter(Completer):
    def get_completions(self, document, complete_event):
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        matches = fuzzyfinder(word_before_cursor, SQLKeywords)
        for m in matches:
            yield Completion(m, start_position=-len(word_before_cursor))


class CommandClient:
    def main(self):
        a = 3
        print("AA")


# try:
#     while True:
#         user_input = prompt(u'npg> ',
#                             history=FileHistory(f'{CONFIG_PATH}/history.txt'),
#                             auto_suggest=AutoSuggestFromHistory(),
#                             completer=SQLCompleter(),
#                             lexer= PygmentsLexer(CypherLexer),
#                             ).strip()
#         try:
#             if user_input == "help":
#                 user_input = "--help"
#             fire.Fire(CommandClient, command=user_input)
#         except:
#             pass
# #         click.echo_via_pager(user_input)
# except (KeyboardInterrupt, EOFError) as e:
#     print("FORCE EXIT<<")


# def cmd():
#     fire.Fire(CommandClient)
