import pandas as pd
import sqlalchemy as sa
from ipykernel.kernelbase import Kernel
import re
from .autocomplete import SQLAutocompleter
import logging
import traceback
from pygments import highlight
from pygments.lexers.python import PythonLexer
from pygments.formatters import HtmlFormatter, TerminalFormatter
from .pygment_error_lexer import SqlErrorLexer
from .style import ThisStyle

__version__ = '0.4.1'

class FixedWidthHtmlFormatter(HtmlFormatter):

    def wrap(self, source):
        return self._wrap_code(source)

    def _wrap_code(self, source):
        yield 0, '<p style="max-width: 120ch;overflow-wrap: break-word;text-align:left">'
        for i, t in source:
            if i == 1:
                # it's a line of formatted code
                t += '<br>'
            yield i, t
        yield 0, '</p>'

class MysqlKernel(Kernel):
    implementation = 'mysql_kernel'
    implementation_version = __version__
    language = 'sql'
    language_version = 'latest'
    language_info = {'name': 'sql',
                     'mimetype': 'text/x-sh',
                     'file_extension': '.sql'}
    banner = 'mysql kernel'

    def __init__(self, **kwargs):
        Kernel.__init__(self, **kwargs)
        self.engine = False
        self.log.setLevel(logging.DEBUG)
        print('Mysql kernel initialized')
        self.log.info('Mysql kernel initialized')
        
    def output(self, output, plain_text = None):
        if plain_text == None:
            plain_text = output
        if not self.silent:
            display_content = {'source': 'kernel',
                               'data': {
                                   'text/html': output,
                                   'text/plain': plain_text,
                                },
                               'metadata': {}}
            self.send_response(self.iopub_socket, 'display_data', display_content)
    
    def ok(self):
        return {'status':'ok', 'execution_count':self.execution_count, 'payload':[], 'user_expressions':{}}

    def err(self, msg):
        return {'status':'error',
                'error':msg,
                'traceback':[msg],
                'execution_count':self.execution_count,
                'payload':[],
                'user_expressions':{}}
    
    def generic_ddl(self, query, msg):
        try:
            with self.engine.begin() as con:
                result = con.execute(sa.sql.text(query))
                split_query = query.split()
                if len(split_query) > 2:
                    object_name = re.match("([^ ]+ ){2}(if (not )?exists )?([^ ]+)", query, re.IGNORECASE).group(4)
                else:
                    object_name = query.split()[1]
                rows_affected = result.rowcount
                if result.rowcount > 0:
                    self.output((msg + '\nRows affected: %d.') % (object_name, rows_affected))
                else:
                    self.output(msg  % (object_name))
                return
        except Exception as msg:
            return self.handle_error(msg)

    def create_db(self, query):
        return self.generic_ddl(query, 'Database %s created successfully.')
        

    def drop_db(self, query):
        return self.generic_ddl(query, 'Database %s dropped successfully.')
        
    def create_table(self, query):
        return self.generic_ddl(query, 'Table %s created successfully.')
        
    def drop_table(self, query):
        return self.generic_ddl(query, 'Table %s dropped successfully.')

    def delete(self, query):
        return self.generic_ddl(query, 'Data deleted from %s successfully.')
    
    def alter_table(self, query):
        return self.generic_ddl(query, 'Table %s altered successfully.')
    
    def insert_into(self, query):
        return self.generic_ddl(query, 'Data inserted into %s successfully.')
    
    def use_db(self, query):
        new_database = re.match("use ([^ ]+)", query, re.IGNORECASE).group(1)
        self.engine = sa.create_engine(self.engine.url.set(database=new_database))
        self.autocompleter = SQLAutocompleter(engine=self.engine, log=self.log)
        return self.generic_ddl(query, 'Changed to database %s successfully.')

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        self.silent = silent
        res = {}
        output = ''
        if not code.strip():
            return self.ok()
        sql = code.rstrip()+('' if code.rstrip().endswith(";") else ';')
        results_raw = None
        try:
            for v in sql.split(";"):
                v = v.rstrip()
                v = re.sub('^[ \r\n\t]+', '', v)
                v = re.sub('\n* *--.*\n', '', v) # remove comments
                l = v.lower()
                if len(l)>0:
                    if l.startswith('mysql://'):
                        if l.count('@')>1:
                            self.output("Connection failed, The Mysql address cannot have two '@'.")
                        else:
                            if v.startswith('mysql://'):
                                v = v.replace('mysql://', 'mysql+pymysql://')
                            self.engine = sa.create_engine(v)
                            self.autocompleter = SQLAutocompleter(engine=self.engine, log=self.log)
                            self.output('Connected successfully!')
                            
                            
                    elif l.startswith('create database '):
                        res = self.create_db(v)
                    elif l.startswith('drop database '):
                        res = self.drop_db(v)
                    elif l.startswith('create table '):
                        res = self.create_table(v)
                    elif l.startswith('drop table '):
                        res = self.drop_table(v)
                    elif l.startswith('delete '):
                        res = self.delete(v)
                    elif l.startswith('alter table '):
                        res = self.alter_table(v)
                    elif l.startswith('use '):
                        res = self.use_db(v)
                    elif l.startswith('insert into '):
                        res = self.insert_into(v)
                    else:
                        if self.engine:
                            v = re.sub('(?<!%)%(?!%)', '%%', v)
                            if l.startswith('select ') and 'limit ' not in l:
                                v = f'{v} limit 1000'
                                results = pd.read_sql(v, self.engine)
                                results_raw = results.to_string()
                                if results.shape[0] == 1000:
                                    output = f'''
                                        <p>Results limitted to 1000 (explicitly add LIMIT to display beyond that)</p>
                                        {results.to_html()}
                                        '''
                                else:
                                    output = results.to_html()
                            else:
                                with self.engine.begin() as con:
                                    execution = con.execute(sa.sql.text(v))
                                    if execution.returns_rows:
                                        results = pd.DataFrame(execution.fetchall(), columns=execution.keys())
                                        output = results.to_html()
                                    elif execution.rowcount > 0:
                                        output = f'Rows affected: {execution.rowcount}'
                                    else:
                                        output = 'No rows affected'
                            output = f'''<div style='max-height: 500px; overflow: auto; width: 100%'>{output}</div>'''
                        else:
                            output = 'Unable to connect to Mysql server. Check that the server is running.'
                        self.output(output, plain_text = results_raw if results_raw else output)
                if res and 'status' in res.keys() and res['status'] == 'error':
                    return res
            return self.ok()
        except Exception as e:
            return self.handle_error(e)
        
    def handle_error(self, e): 
        search_res = re.search(r'\d+,[^"\']*["\']([^"\']+)', e.args[0])
        msg = str(e)
        if search_res and search_res.lastindex >= 1:
            msg = search_res.group(1)

        # Convert to HTML with Pygments
        formatter = FixedWidthHtmlFormatter(full=True, style=ThisStyle, traceback=False)
        tb_html = highlight("ERROR: " + msg, SqlErrorLexer(), formatter)
        tb_terminal = highlight(msg, SqlErrorLexer(), TerminalFormatter())

        # Send formatted traceback as an HTML response
        self.send_response(
                self.iopub_socket,
                "display_data",
                {
                    "data": {
                        "text/html": tb_html,
                        "text/plain": tb_terminal
                    },
                    "metadata": {}
                },
            )
        return {"status": "error", "execution_count": self.execution_count}
    
    def do_complete(self, code, cursor_pos):
        self.log.info('Try to autocomplete')
        if not self.autocompleter:
            return {"status": "ok", "matches": []}
        completion_list = self.autocompleter.get_completions(code, cursor_pos)
        # match_text_list = [completion.text for completion in completion_list]
        # offset = 0
        # if len(completion_list) > 0:
        #     offset = completion_list[
        #         0
        #     ].start_position  # if match part is 'sel', then start_position would be -3
        # type_dict_list = []
        # for completion in completion_list:
        #     if completion.display_meta is not None:
        #         type_dict_list.append(
        #             {
        #                 "start": completion.start_position,
        #                 "end": len(completion.text) + completion.start_position,
        #                 "text": completion.text,
        #                 # display_meta is FormattedText object
        #                 "type": completion.display_meta_text,
        #             }
        #         )

        cursor_offset = 0

        word_completing = re.search('[^ ]+$',code[:cursor_pos])
        if word_completing:
            cursor_offset += len(word_completing.group(0))

        return {
            "status": "ok",
            "matches": completion_list,
            "cursor_start": cursor_pos - cursor_offset,
            "cursor_end": cursor_pos,
            "metadata": {},
        }
