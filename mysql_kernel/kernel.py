import pandas as pd
import sqlalchemy as sa
from ipykernel.kernelbase import Kernel
import re


__version__ = '0.4.1'

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
        
    def output(self, output):
        if not self.silent:
            display_content = {'source': 'kernel',
                               'data': {'text/html': output},
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
            self.output(str(msg))
            return

    def create_db(self, query):
        self.generic_ddl(query, 'Database %s created successfully.')
        

    def drop_db(self, query):
        self.generic_ddl(query, 'Database %s dropped successfully.')
        
    def create_table(self, query):
        self.generic_ddl(query, 'Table %s created successfully.')
        
    def drop_table(self, query):
        self.generic_ddl(query, 'Table %s dropped successfully.')

    def delete(self, query):
        self.generic_ddl(query, 'Data deleted from %s successfully.')
    
    def alter_table(self, query):
        self.generic_ddl(query, 'Table %s altered successfully.')
    
    def insert_into(self, query):
        self.generic_ddl(query, 'Data inserted into %s successfully.')
    
    def use_db(self, query):
        self.generic_ddl(query, 'Changed to database %s successfully.')

    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        self.silent = silent
        output = ''
        if not code.strip():
            return self.ok()
        sql = code.rstrip()+('' if code.rstrip().endswith(";") else ';')
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
                            self.engine = sa.create_engine(f'mysql+py{v}')
                            self.output('Connected successfully!')
                    elif l.startswith('create database '):
                        self.create_db(v)
                    elif l.startswith('drop database '):
                        self.drop_db(v)
                    elif l.startswith('create table '):
                        self.create_table(v)
                    elif l.startswith('drop table '):
                        self.drop_table(v)
                    elif l.startswith('delete '):
                        self.delete(v)
                    elif l.startswith('alter table '):
                        self.alter_table(v)
                    elif l.startswith('use '):
                        self.use_db(v)
                    elif l.startswith('insert into '):
                        self.insert_into(v)
                    else:
                        if self.engine:
                            v = re.sub('(?<!%)%(?!%)', '%%', v)
                            if l.startswith('select ') and 'limit ' not in l:
                                v = f'{v} limit 1000'
                                results = pd.read_sql(v, self.engine)
                                if results.shape[0] == 1000:
                                    output = f'''
                                        <p>Results limitted to 1000 (explicitly add LIMIT to display beyond that)</p>
                                        {results.to_html()}
                                        '''
                                else:
                                    output = results.to_html()
                            else:
                                output = pd.read_sql(v, self.engine).to_html()
                            output = f'''<div style='max-height: 500px; overflow: auto; width: 100%'>{output}</div>'''
                        else:
                            output = 'Unable to connect to Mysql server. Check that the server is running.'
                        self.output(output)
            return self.ok()
        except Exception as msg:
            self.output(str(msg))
            return self.err('Error executing code ' + sql)
