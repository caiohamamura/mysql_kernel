from pygments.lexer import RegexLexer, bygroups
from pygments.token import *
import re

__all__ = ['SqlErrorLexer']

class SqlErrorLexer(RegexLexer):
    """
    A lexer for highlighting errors from MySQL/MariaDB.
    """

    name = 'sqlerror'
    aliases = ['sqlerror']
    filenames = ['*.sqlerror']

    flags = re.IGNORECASE
    tokens = {
        'root': [
            (r"^ERROR", Token.Generic.Deleted),
            (r"(?<=table [\"'])[^\"']+", Token.Name.Class),
            (r"(?<=relation [\"'])[^\"']+", Token.Name.Class),
            (r"(?<=column [\"'])[^\"']+", Token.Name.Class),
            (r"(?<=column )[^ \"]+", Token.Name.Class),
            (r"(?<=database [\"'])[^\"']+", Token.Name.Class),
            (r"(?<=near [\"'])[^\"']+", Token.Name.Builtin),
            (r"line [0-9]+", Token.Number),
        ]
    }

