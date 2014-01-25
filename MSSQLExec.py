import sys
import os
import threading 
import time
from datetime import date

import sublime
import sublime_api
import sublime_plugin

sys.path.append(os.path.dirname(sys.executable))
sys.path.append(os.path.join(os.path.dirname(__file__), "lib", "prettytable"))
sys.path.append(os.path.join(os.path.dirname(__file__), "lib")) #TODO
if sublime.arch() == 'x64':
	sys.path.append(os.path.join(os.path.dirname(__file__), "lib", "pymssql", "x64"))
elif sublime.arch() == 'x32':
	sys.path.append(os.path.join(os.path.dirname(__file__), "lib", "pymssql", "x32"))

import prettytable
import pymssql

## We can't pass object between views, so we use this global var
temp_results = []

## 
## self.view.settings().get('default_line_ending')
##

class TsqlResultCommand(sublime_plugin.TextCommand):
	""" Represent query result """

	def run(self, edit, string = None):
		global temp_results
		query_results = temp_results

		view_settings = self.view.settings()
		if view_settings.get("word_wrap"):
			self.view.run_command("toggle_setting", { "setting": "word_wrap"})
		
		self.view.settings().set("font_size", "7")
		self.view.set_scratch(True)

		if string is not None:
			self.view.insert(edit, self.view.size(), string)	
			return

		for result in query_results:
			if result.has_error():
				self.draw_error(edit, result)
			elif result.has_rows():
				self.draw_rows(edit, result)

		temp_results = None
		
	def draw_error(self, edit, result):
		self.view.insert(edit, self.view.size(), "\n--------------\nError in query:\n")
		self.view.insert(edit, self.view.size(), result.query + "\n")
		self.view.insert(edit, self.view.size(), str(result.error))

	def draw_rows(self, edit, result):
		if result.menumode:
			self.view.insert(edit, self.view.size(), "\n")
			self.view.insert(edit, self.view.size(), result.query)
			self.view.insert(edit, self.view.size(), "\n")	
		if len(str(result)) > 0:
			self.view.insert(edit, self.view.size(), "\n")
			self.view.insert(edit, self.view.size(), str(result))


class TsqlExecCommand(sublime_plugin.TextCommand):

	def run(self, edit):
		settings = sublime.load_settings("MSSQLExec.sublime-settings")	

		thread = SQLScriptRunner(settings, self.get_sql_queries())
		thread.start()
		self.handle_thread(edit, thread)

	def get_sql_queries(self):
		view = self.view
		region = view.sel()[0]
		selection = view.substr(view.size())
		if not region.empty():
			selection = view.substr(region)
		queries = selection.split('go')
		queries = [x for x in queries if x]
		print(queries)
		return queries

	def handle_thread(self, edit, thread, status=0):
		if thread.is_alive():
			progress_string = ["["," ", " ", " ", " ", " ", "]"]
			progress_string.insert(status % 6 + 1, "=")
			self.update_status("Executing query " + "".join(progress_string))
			sublime.set_timeout(lambda: self.handle_thread(edit, thread, status+1), 100)
			return			

		need_to_show_result = False
		for result in thread.results:
			if result.has_rows() or result.has_error():
				need_to_show_result = True

		if need_to_show_result:
			self.draw_result(edit, thread.results)
			self.clear_status()
		else:
			self.update_status("Executed succesfully!")

	def update_status(self, msg):
		self.view.set_status("tsqlexec", msg)		

	def clear_status(self):
		print("erase status")
		self.view.erase_status("tsqlexec")

	def draw_result(self, edit, results):
		output = get_result_tab(self.view)
		global temp_results
		temp_results = results
		output.run_command("tsql_result")
		self.view.window().focus_view(output)

def get_result_tab(view):
	output = None
	output_name = "Query Result:"
	for view in view.window().views():
		if view.name() == output_name:
			output = view
			break

	if output is None:
		output = view.window().new_file()
		output.set_name(output_name)
	return output

class QueryResult(object):

	def __init__(self, query, cursor, error):
		self.query = query
		self.cursor = cursor
		self.error = error
		self.menumode = False
		if cursor is not None and not self.has_error():
			self.table_view = prettytable.from_db_cursor(cursor)

	def has_rows(self):
		if self.cursor is not None:
			return self.cursor.rowcount

	def has_error(self):
		return self.error is not None

	def __str__(self):
		if self.table_view is not None:
			return self.table_view.get_string()
		else:
			return ""

class QueryExecutor(object):

	def __init__(self, query=None, connection=None):
		self.query = query
		self.connection = connection
		self.result = None
		self.menumode = False

	def execute(self):
		try:
			cursor = self.connection.cursor()
			cursor.execute(self.query)
			self.result = QueryResult(self.query, cursor, None)
		except pymssql.Error as err:
			self.result = QueryResult(self.query, None, err)

		self.result.menumode = self.menumode

	def set_menumode(self, b):
		self.menumode = b

class Singleton(type):
	_instances = {}
	def __call__(cls, *args, **kwargs):
		if cls not in cls._instances:
			cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
		return cls._instances[cls]
		
class DBConnection(object):

	def __init__(self, settings):
		self.settings = settings	
		self.connection = None
		self.connect()

	def connect(self):
		active_server = self.settings.get("active_server")
		server_settings = self.settings.get("servers").get(active_server)

		db_engine = server_settings.get("db_engine")
		server = server_settings.get("server")
		dbname = server_settings.get("dbname")
		username = server_settings.get("username")
		password = server_settings.get("password")

		## TODO: get connection through a specific db connector
		try:
			self.connection = pymssql.connect(server=server, 
				user=username, 
				password=password, 
				database=dbname, 
				timeout=0, 
				login_timeout=60, 
				charset='UTF-8', 
				as_dict=False, 
				host='', 
				appname=None, 
				port='1433')
			self.connection.autocommit(True)
		except pymssql.OperationalError as e:
			pass

	def close(self):
		self.connection.close()
		self.instance = None

	def cursor(self):
		return self.connection.cursor()

	def commit(self):
		self.connection.commit()


class SQLScriptRunner(threading.Thread):

	def __init__(self, settings, queries, menumode=False):
		self.settings = settings
		self.queries = queries
		self.results = []
		self.menumode = menumode
		threading.Thread.__init__(self)

	def run(self):
		connection = DBConnection(self.settings)
		for query in self.queries:
			query_executor = QueryExecutor(query, connection)
			query_executor.set_menumode(self.menumode)
			query_executor.execute()
			self.results.append(query_executor.result)

class TsqlMenuCommand(TsqlExecCommand):
	__table_actions__ = ["Select Top 1000", "DDL"]

	def run(self, edit):
		tables_list = [] #TODO: added expiration cache

		def load_tables_list():
			print("load tables")
			settings = sublime.load_settings("MSSQLExec.sublime-settings")	
			con = DBConnection(settings)
			cursor = con.cursor()
			cursor.execute("select distinct TABLE_NAME from information_schema.tables")
			for row in cursor:
				tables_list.append(row[0])

		def show_table_action(rs):
			if(rs == -1):
				return
			self.view.settings().set("selection", rs)
			sublime.set_timeout(lambda: self.view.window().show_quick_panel(TsqlMenuCommand.__table_actions__, do_table_action), 2)
		
		def do_table_action(rs):
			if rs == 0:
				select()
			if rs == 1:
				ddl()

		def select():
			selection = self.view.settings().get("selection")
			table_name = tables_list[selection]
			q = "select top 100 * from " + table_name
			settings = sublime.load_settings("MSSQLExec.sublime-settings")
			thread = SQLScriptRunner(settings, [q], menumode=True)
			thread.start()
			self.handle_thread(edit, thread)

		def ddl():
			selection = self.view.settings().get("selection")
			table_name = tables_list[selection]
			q = "select COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH " + "from INFORMATION_SCHEMA.COLUMNS where table_name='" + table_name + "' ORDER BY COLUMN_NAME"	
			con = DBConnection(sublime.load_settings("MSSQLExec.sublime-settings"))
			con.cursor().execute(q)
			ddl = []
			for row in con.cursor():
				col_name = row[0]
				col_null = row[1]
				col_type = row[2]
				col_length = row[3]
				ddl.append(col_name + " " + str(col_type) + "(" + str(col_length) + ") " + str(col_null) + "\n")
			re = "".join(ddl)
			re = table_name + "\n" + re
			output = get_result_tab(self.view)
			output.run_command("tsql_result", {'string' : re})

		if len(tables_list) == 0:
			load_tables_list()

		self.view.window().show_quick_panel(tables_list, show_table_action)