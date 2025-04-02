from pygments.lexer import RegexLexer, bygroups
from pygments.token import *

__all__ = ['SqlErrorLexer']

class SqlErrorLexer(RegexLexer):
    """
    A lexer for highlighting errors from MySQL/MariaDB.
    """

    name = 'sqlerror'
    aliases = ['sqlerror']
    filenames = ['*.sqlerror']

    tokens = {
        'root': [
            (r"(ERROR)(.*Table ')([^']+)", bygroups(Token.Generic.Deleted, Token.Generic, Token.Name.Class)),
            (r"(ERROR)(.*column ')([^']+)(.*in ')([^']+)", bygroups(Token.Generic.Deleted, Token.Generic, Token.Name.Class, Token.Generic, Token.Name.Builtin)),
            (r"(ERROR)(.*near ')([^']+)(.*line [0-9]+)", bygroups(Token.Generic.Deleted, Token.Generic, Token.Name.Builtin, Token.Number)),
            (r"(ERROR)(.*database ')([^']+)", bygroups(Token.Generic.Deleted, Token.Generic, Token.Name.Class)),
        ]
    }

