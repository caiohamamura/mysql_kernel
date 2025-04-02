import re
from sqlalchemy import inspect

class SQLAutocompleter:
    def __init__(self, engine, log):
        """
        Initializes the autocompleter with an SQLAlchemy engine.
        
        Parameters:
        - engine: SQLAlchemy engine connected to a database.
        """
        self.engine = engine
        self.inspector = inspect(engine)
        self.default_schema = self.inspector.default_schema_name
        self.log = log
        self.log.info(f"Autocompleter initialized with engine: {engine}") 
        
    def get_real_previous_keyword(self, tokens):
        """
        Identifies the real previous keyword in SQL syntax, i.e., section delimiters like `SELECT`, `FROM`, `WHERE`.
        
        Parameters:
        - tokens (list): List of tokens (words) before the cursor position.
        
        Returns:
        - str: The most recent real SQL keyword (e.g., `SELECT`, `FROM`).
        """
        sql_keywords = {
            "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "INSERT", "UPDATE", "DELETE",
            "JOIN", "ON", "LIMIT", "DISTINCT", "SET"
        }
        for token in reversed(tokens):
            if token.upper() in sql_keywords:
                return token.upper()
        return ""


    def get_completions(self, code, cursor_pos):
        """
        Returns autocompletions based on the SQL context.
        
        Parameters:
        - code (str): Full SQL query being typed.
        - cursor_pos (int): Cursor position in the query.
        
        Returns:
        - list: Suggested completions.
        """
        preceding_text = code[:cursor_pos]
        tokens = re.findall(r"[^ ;\(\)\r\n\t,]+", preceding_text, re.IGNORECASE)
        previous_keyword = self.get_real_previous_keyword(tokens)
        previous_word = tokens[-1].upper() if tokens else ""
        is_preceding_comma = preceding_text.rstrip().endswith(",")
        is_preceding_space = preceding_text.endswith(" ")
        is_completing_word = preceding_text[-1].isalpha()
        current_completing = ''
        if is_completing_word:  
            previous_word = tokens[-2].upper() if len(tokens) > 1 else ""
            current_completing = tokens[-1].upper() if tokens else ""

        if previous_keyword == "SELECT":
            if is_preceding_comma == False and is_preceding_space == True and previous_word != "SELECT":
                completions = ["FROM"]
            else:
                completions = self.get_columns(code) + self.get_functions()
        elif previous_keyword  in {"FROM", "JOIN"}:
            completions = self.get_tables()
        elif previous_keyword  == "WHERE":
            completions = self.get_columns(code) + self.get_functions()
        elif previous_word  == "GROUP":
            completions = ["BY"]
        elif previous_word  == "ORDER":
            completions = ["BY"]
        elif previous_word == "INSERT":
            completions = ["INTO"]
        elif previous_word == "UPDATE":
            completions = self.get_tables()
        elif previous_keyword == 'UPDATE':
            completions = ["SET"]
            completions += self.get_tables()
        elif previous_word == "DELETE":
            completions = ["FROM"]
        elif previous_word  == "DISTINCT":
            completions = self.get_columns(code)
        elif previous_keyword == "DISTINCT":
            completions = self.get_columns(code) + self.get_functions()
        elif previous_keyword in {"GROUP", "ORDER"}:
            completions = self.get_columns(code)
        elif previous_keyword  == "HAVING":
            completions = self.get_columns(code) + self.get_functions()
        elif previous_keyword  == "SET":
            completions = self.get_columns(code)
        elif previous_word  == "VALUES":
            completions = '('
        elif previous_keyword == "VALUES":
            completions = self.get_columns(code)
        elif previous_word in {"INNER", "LEFT", "RIGHT", "FULL"}:
            completions = ["JOIN"]
        elif previous_keyword == "DISTINCT" or previous_keyword == "LIMIT" or previous_keyword == "OFFSET":
            completions = []
        else:
            completions = self.get_sql_keywords()

        if is_completing_word:
            filter_func = lambda x: x.lower().startswith(current_completing.lower())
            if is_preceding_comma == False and is_preceding_space == False:
                filtered_suggestions = [suggestion for suggestion in completions if filter_func(suggestion)]
                return sorted(filtered_suggestions)

        return completions
    

    def get_tables(self):
        """b
        Returns a list of available tables, excluding the default schema.
        
        Returns:
        - list: Tables without default schema.
        """
        
        schemas = self.inspector.get_schema_names()
        tables = self.inspector.get_table_names(schema=self.default_schema)  # Get tables in default schema

        if self.default_schema:
            for schema in schemas:
                schema_tables = self.inspector.get_table_names(schema=schema)

                if schema != self.default_schema:
                    tables.extend([f"{schema}.{table}" for table in schema_tables])  # Keep schema.table

        return tables

    def get_columns(self, code):
        """
        Extracts tables from the query and returns relevant columns.
        
        Parameters:
        - code (str): SQL query.

        Returns:
        - list: Column names from the tables used in the query.
        """
        table_names = self.extract_table_names(code)
        columns = []
        for table in table_names:
            schema, table_name = self.split_schema_table(table)
            try:
                table_columns = [col["name"] for col in self.inspector.get_columns(table_name, schema=schema)]
                columns.extend(table_columns)
            except Exception:
                pass  # Ignore missing tables
        return columns

    def get_functions(self):
        """Returns common SQL functions."""
        return [
            "COUNT()", "AVG()", "SUM()", "MIN()", "MAX()", 
            "LOWER()", "UPPER()", "NOW()", "DATE()", "ROUND()"
        ]

    def get_sql_keywords(self):
        """Returns a list of common SQL keywords."""
        return [
            "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING",
            "INSERT INTO", "VALUES", "UPDATE", "SET", "DELETE FROM",
            "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN",
            "ON", "DISTINCT", "LIMIT", "OFFSET"
        ]

    def extract_table_names(self, code):
        """
        Extracts table names (including schema-qualified) from an SQL query.
        
        Parameters:
        - code (str): SQL query.
        
        Returns:
        - list: Table names found in the query.
        """
        matches = re.findall(r"FROM\s+([\w.]+)|JOIN\s+([\w.]+)|UPDATE\s+([\w.]+)", code, re.IGNORECASE)
        return [table for tup in matches for table in tup if table]

    def split_schema_table(self, table):
        """
        Splits a schema-qualified table into schema and table parts.
        
        Parameters:
        - table (str): Table name (could be schema-qualified like 'schema.table').

        Returns:
        - tuple: (schema, table_name) or (None, table_name) if no schema.
        """
        parts = table.split(".")
        if len(parts) == 2:
            schema, table_name = parts
            if schema == self.default_schema:  # Remove default schema
                return None, table_name
            return schema, table_name
        return self.default_schema, table  # No schema


# Example usage
# from sqlalchemy import create_engine
# engine = create_engine("postgresql://postgres@localhost/tume")
# completer = SQLAutocompleter(engine)
# completions = completer.get_completions("SELECT municipio,  FROM tume.cadastro", 17)
# print(completions)